#!/usr/bin/env python3
"""
classify_ena_samples.py
=======================

Classify ENA sample metadata for three things:

  1. disease   -- the disease the sample is investigating
  2. is_control -- whether the sample is a control (yes / no / unknown)
  3. is_tumor   -- whether the sample is a tumor (yes / no / unknown);
                   any tumor forces disease -> cancer

It works on CSV exports that contain the ENA default sample checklist columns
plus a ``custom_attributes`` column holding a JSON object of study-specific
attributes. It reads both the standard columns and the JSON, applies an
evidence-based heuristic, and writes every original column back out with four
new columns appended: ``disease``, ``is_control``, ``is_tumor`` and
``classification_evidence`` (which records the field+value that drove each call
so any row can be audited).

WHY A TWO-PASS DESIGN
---------------------
Disease is partly a *study-level* property: a healthy participant in a
colorectal-cancer study is still part of a colorectal-cancer study. So pass 1
scans every input row to learn each study's dominant disease (from genuine case
samples only), behind a gate that refuses to propagate a tiny tagged minority in
a large mixed population cohort. Pass 2 then writes the output, filling a
sample's disease from its own metadata first and falling back to the study-level
disease for controls / unlabeled samples in disease-focused studies.

USAGE
-----
    # classify some shards into one combined CSV
    python classify_ena_samples.py data/part0*.csv --output out/classified.csv

    # gzip the combined output
    python classify_ena_samples.py data/*.csv -o out/classified.csv --compress gzip

    # zip it instead
    python classify_ena_samples.py data/*.csv -o out/classified.csv --compress zip

    # write one augmented file per input instead of a single combined file
    python classify_ena_samples.py data/*.csv -o out/ --mode per-file

    # classify only the new shards, but compute study-level disease votes over
    # the WHOLE dataset so labels stay consistent with earlier batches
    python classify_ena_samples.py data/part01*.csv -o out/new.csv \
        --vote-scope "data/part0*.csv" "data/part01*.csv"

The script streams the files in chunks, so memory use stays low even on
multi-hundred-MB inputs.

NOTE
----
This is a transparent heuristic over inconsistently-annotated public metadata,
not paper-level ground truth. The ``classification_evidence`` column makes every
call auditable, and the rule tables below are meant to be edited.
"""

import argparse
import glob
import gzip
import json
import os
import re
import sys
import zipfile
from collections import Counter, defaultdict

import pandas as pd

# ============================================================================
# RULE TABLES  (edit these to extend coverage)
# ============================================================================

# Disease canonicalisation. Order matters: more specific patterns first.
# Each entry: (regex on lowercased text) -> canonical disease label.
DISEASE_PATTERNS = [
    # --- specific cancers (multi-word, safe to scan in free text) ---
    (r'\bcolorectal cancer\b|colorectal (adeno)?carcinoma', 'colorectal cancer'),
    (r'colitis[- ]associated cancer', 'colitis-associated cancer'),
    (r'esophageal squamous cell carcinoma', 'esophageal squamous cell carcinoma'),
    (r'esophageal (adeno)?carcinoma|esophagogastric junction', 'esophageal/gastroesophageal cancer'),
    (r'gastric cancer|stomach cancer|gastric (adeno)?carcinoma', 'gastric cancer'),
    (r'lung adenocarcinoma|lung cancer|non[- ]small cell lung', 'lung cancer'),
    (r'\bhepatocellular|liver cancer', 'hepatocellular carcinoma'),
    (r'pancreatic cancer|pancreatic (adeno)?carcinoma', 'pancreatic cancer'),
    (r'breast cancer|breast (adeno)?carcinoma', 'breast cancer'),
    (r'prostate cancer|castrate resistant prostate', 'prostate cancer'),
    (r'endometrial cancer|endometrial carcinoma', 'endometrial cancer'),
    (r'ovarian cancer|ovarian carcinoma', 'ovarian cancer'),
    (r'cervical cancer|cervical carcinoma', 'cervical cancer'),
    (r'\bhead and neck\b.*(cancer|carcinoma)', 'head and neck cancer'),
    (r'oral (squamous )?cell carcinoma|oral cancer', 'oral cancer'),
    (r'bladder cancer|urothelial carcinoma', 'bladder cancer'),
    (r'\bcholangiocarcinoma\b|bile duct cancer', 'cholangiocarcinoma'),
    (r'\bmelanoma\b', 'melanoma'),
    (r'acute myeloid leukemia|acute myeloid leukaemia', 'acute myeloid leukemia'),
    (r'acute lymphoblastic leukemia|acute lymphoblastic leukaemia', 'acute lymphoblastic leukemia'),
    (r'\bleukemia\b|\bleukaemia\b', 'leukemia'),
    (r'\blymphoma\b', 'lymphoma'),
    (r'\bglioma\b|glioblastoma', 'glioma'),
    (r'\bneuroblastoma\b', 'neuroblastoma'),
    (r'\bsquamous cell carcinoma\b', 'squamous cell carcinoma'),
    (r'\badenocarcinoma\b', 'adenocarcinoma'),
    (r'\bcarcinoma\b|\bcancer\b|\bmalignan|\bneoplasm|\btumou?r\b|\bmetasta', 'cancer'),
    (r'\badenoma\b|\bpolyp\b', 'adenoma/polyp'),

    # --- IBD / GI ---
    (r"crohn'?s? disease|crohn'?s? ?$|colonic crohn|ileal crohn", "Crohn's disease"),
    (r'ulcerative colitis', 'ulcerative colitis'),
    (r'inflammatory bowel disease', 'inflammatory bowel disease'),
    (r'\bpouchitis\b', 'pouchitis'),
    (r'irritable bowel syndrome', 'irritable bowel syndrome'),
    (r'celiac|coeliac', 'celiac disease'),
    (r'clostridi(oides|um) difficile|\bc\.? ?diff', 'C. difficile infection'),
    (r'\bdiverticul', 'diverticular disease'),

    # --- metabolic / endocrine ---
    (r'type 1 diabetes|type i diabetes', 'type 1 diabetes'),
    (r'type 2 diabetes|type ii diabetes', 'type 2 diabetes'),
    (r'gestational diabetes', 'gestational diabetes'),
    (r'\bdiabetes\b|\bdiabetic\b', 'diabetes'),
    (r'\bobesity\b|\bobese\b', 'obesity'),
    (r'metabolic syndrome', 'metabolic syndrome'),
    (r'non[- ]alcoholic fatty liver|\bnafld\b|\bnash\b', 'non-alcoholic fatty liver disease'),
    (r'\bcirrhosis\b', 'cirrhosis'),
    (r'\bthyroid', 'thyroid disease'),

    # --- infectious / respiratory ---
    (r'covid[- ]?19|sars-?cov-?2|\bcovid\b', 'COVID-19'),
    (r'\btuberculosis\b', 'tuberculosis'),
    (r'human immunodeficiency', 'HIV'),
    (r'\bhepatitis\b', 'hepatitis'),
    (r'\bsepsis\b|\bseptic\b', 'sepsis'),
    (r'\bpneumonia\b', 'pneumonia'),
    (r'cystic fibrosis', 'cystic fibrosis'),
    (r'chronic obstructive pulmonary', 'COPD'),
    (r'\basthma\b', 'asthma'),
    (r'\bbronchiectasis\b', 'bronchiectasis'),
    (r'cutaneous ulcer', 'cutaneous ulcer disease'),
    (r'\bmalaria\b', 'malaria'),
    (r'\bcholera\b', 'cholera'),
    (r'gulf war illness', 'Gulf War illness'),

    # --- autoimmune / inflammatory ---
    (r'rheumatoid arthritis', 'rheumatoid arthritis'),
    (r'multiple sclerosis', 'multiple sclerosis'),
    (r'systemic lupus|\blupus\b', 'lupus'),
    (r'psoriasis|psoriatic', 'psoriasis'),
    (r'atopic dermatitis|\beczema\b', 'atopic dermatitis'),
    (r'ankylosing spondylitis|axial spondyloarthritis', 'spondyloarthritis'),
    (r'chronic recurrent multifocal osteomyelitis', 'CRMO'),

    # --- neuro / psych ---
    (r"alzheimer", "Alzheimer's disease"),
    (r"parkinson", "Parkinson's disease"),
    (r'autism spectrum', 'autism spectrum disorder'),
    (r'\badhd\b|add[/ ]adhd', 'ADHD'),
    (r'major depressive|\bdepression\b', 'depression'),
    (r'\bmigraine\b', 'migraine'),
    (r'\bepilepsy\b|\bseizure', 'epilepsy'),
    (r'cavernous angioma|cerebral cavernous', 'cerebral cavernous malformation'),

    # --- oral / dental ---
    (r'\bcaries\b|dental cavit', 'dental caries'),
    (r'periodonti', 'periodontitis'),

    # --- other ---
    (r'kidney disease|chronic kidney|renal disease', 'kidney disease'),
    (r'cardiovascular disease|coronary artery', 'cardiovascular disease'),
    (r'\ballerg', 'allergy'),
    (r'\bpreeclampsia\b|pre-eclampsia', 'preeclampsia'),
    (r'\bendometriosis\b', 'endometriosis'),
    (r'bacterial vaginosis', 'bacterial vaginosis'),
    (r'\bstone-?\d', 'kidney/bile stone'),
]
DISEASE_RE = [(re.compile(p), lab) for p, lab in DISEASE_PATTERNS]

# Short abbreviations: ONLY trusted as an EXACT value of a dedicated disease
# field (host_disease/disease/diagnosis/...), never scanned in free text.
ABBREV_MAP = {
    'cd': "Crohn's disease", 'uc': 'ulcerative colitis',
    'ibd': 'inflammatory bowel disease', 'crc': 'colorectal cancer',
    'cac': 'colitis-associated cancer', 'escc': 'esophageal squamous cell carcinoma',
    'egj': 'esophageal/gastroesophageal cancer', 't1d': 'type 1 diabetes',
    't2d': 'type 2 diabetes', 'aml': 'acute myeloid leukemia',
    'b-all': 'acute lymphoblastic leukemia', 't-all': 'acute lymphoblastic leukemia',
    'ra': 'rheumatoid arthritis', 'ms': 'multiple sclerosis', 'rrms': 'multiple sclerosis',
    'sle': 'lupus', 'cf': 'cystic fibrosis', 'copd': 'COPD', 'tb': 'tuberculosis',
    'hiv': 'HIV', 'cdi': 'C. difficile infection', 'cdi positive': 'C. difficile infection',
    'ibs': 'irritable bowel syndrome', 'gwi': 'Gulf War illness',
    'nafld': 'non-alcoholic fatty liver disease', 'nash': 'non-alcoholic fatty liver disease',
    'hcc': 'hepatocellular carcinoma', 'pdac': 'pancreatic cancer', 'oscc': 'oral cancer',
    'hnscc': 'head and neck cancer', 'gbm': 'glioma', 'asd': 'autism spectrum disorder',
    'ckd': 'kidney disease', 'cvd': 'cardiovascular disease', 'bv': 'bacterial vaginosis',
    'crmo': 'CRMO', 't2dm': 'type 2 diabetes', 't1dm': 'type 1 diabetes',
}

# Labels that ARE cancers, so a tumor sample's disease becomes/keeps a cancer.
CANCER_LABELS = {
    'colorectal cancer', 'colitis-associated cancer', 'esophageal squamous cell carcinoma',
    'esophageal/gastroesophageal cancer', 'gastric cancer', 'lung cancer', 'hepatocellular carcinoma',
    'pancreatic cancer', 'breast cancer', 'prostate cancer', 'endometrial cancer', 'ovarian cancer',
    'cervical cancer', 'head and neck cancer', 'oral cancer', 'bladder cancer', 'cholangiocarcinoma',
    'melanoma', 'acute myeloid leukemia', 'acute lymphoblastic leukemia', 'leukemia', 'lymphoma',
    'glioma', 'neuroblastoma', 'squamous cell carcinoma', 'adenocarcinoma', 'cancer',
}
# adenoma/polyp is a precancerous lesion, not cancer -> intentionally NOT here

# Values meaning "no disease / not a disease name" -> never used as a disease label.
# Includes disease-activity states and trial responses that are NOT diseases.
NULLISH = {'', 'na', 'n/a', 'nan', 'none', 'not applicable', 'not collected',
           'not provided', 'missing', 'unknown', 'unspecified', 'unclassified',
           'control', 'normal', 'healthy', 'healthy control', 'hc', 'hv',
           'i do not have this condition', '0', 'no', 'baseline', 'other',
           'benign', 'indeterminate', 'indefinite', 'responder', 'non-responder',
           'nonresponder', 'responders', 'stable', 'stable_disease', 'exacerbation',
           'active', 'inactive', 'active disease', 'inactive disease', 'remission',
           'mild', 'moderate', 'severe', 'positive', 'negative', 'yes', 'case',
           'disease', 'diseased', 'm', 'k', 'b', 't', 'post-bcg', 'pre-bcg',
           'inflammatory', 'stricturing', 'penetrating', 'stricturing and penetrating',
           'true', 'false', 'affected'}

# Values in a *disease-status* field that explicitly mean the subject is healthy.
HEALTHY_STATUS = {'none', 'no', 'healthy', 'healthy control', 'hc', 'hv', 'normal',
                  'unaffected', 'non-ibd', 'non ibd', 'nonibd', 'negative', 'control'}

# ---- control detection -------------------------------------------------------
CONTROL_VALUES = {
    'control', 'healthy', 'healthy control', 'normal', 'hc', 'hv', 'health', 'controls',
    'healthy_control', 'non-tumor', 'non tumor', 'nontumor', 'non-cancer', 'adjacent normal',
    'normal adjacent', 'negative control', 'control blank', 'blank', 'mock', 'sham',
    'no', 'negative', 'ctl', 'ctrl', 'tcon', 'con', 'labcontrol test', 'subset_healthy',
    'unaffected', 'non-ibd', 'non ibd', 'nonibd', 'high risk normal',
}
CONTROL_RE = re.compile(r'\b(healthy control|negative control|control blank|adjacent normal|'
                        r'normal adjacent|non[- ]?tumou?r|non[- ]?cancer|non[- ]?ibd|'
                        r'sham|mock control)\b')
# fields, in priority order, whose value may indicate control status
CONTROL_FIELDS = ['subset_healthy', 'case_control', 'case_or_control', 'group', 'sample_type',
                  'sampletype', 'samplegroup', 'treatment_group', 'study_group', 'cohort',
                  'host_disease', 'disease', 'diagnosis', 'phenotype', 'host_phenotype',
                  'host_disease_status', 'condition', 'clinical_condition', 'disease_state',
                  'site', 'tissue', 'title', 'isolation_source', 'tissue_type', 'description']

# ---- tumor detection ---------------------------------------------------------
# NOTE: 'title'/'description'/'scientific_name' are EXCLUDED -- they often carry
# study-level boilerplate (e.g. "Normal and tumour CRC samples") identical on
# every row, which would mass-mislabel mixed studies. Tumor must come from a
# per-sample field.
TUMOR_POS_RE = re.compile(r'\b(tumou?r|carcinoma|adenocarcinoma|malignan|neoplasm|cancerous|'
                          r'cancer tissue|tumor tissue|cancer biopsy)\b')
TUMOR_NEG_RE = re.compile(r'\b(non[- ]?tumou?r|non[- ]?cancer|adjacent (normal|tissue)|'
                          r'normal adjacent|peritumou?r|tumor[- ]?free|tumour[- ]?free|'
                          r'non[- ]?malignant|benign)\b')
TUMOR_FIELDS = ['is_tumor', 'tumor', 'tumour', 'site', 'tissue', 'tissue_type', 'sample_type',
                'sampletype', 'group', 'diagnosis', 'host_disease', 'disease',
                'histological_type', 'isolation_source']

# Standard checklist columns that carry free text worth scanning.
STD_TEXT_FIELDS = ['title', 'isolation_source', 'tissue_type', 'scientific_name', 'description',
                   'host', 'cell_line', 'specimen_voucher', 'bio_material']

DISEASE_FIELDS_DEDICATED = ['study_disease', 'host_disease', 'disease', 'diagnosis_full',
                            'diagnosis', 'ibd_diagnosis_refined', 'ibd_diagnosis',
                            'host_phenotype', 'phenotype', 'host_disease_status',
                            'clinical_condition', 'condition', 'disease_state',
                            'cardiometabolic_status']
DISEASE_VERBATIM_OK = ('study_disease', 'host_disease', 'disease', 'diagnosis',
                       'diagnosis_full', 'ibd_diagnosis', 'ibd_diagnosis_refined')
NO_DISEASE = 'not specified / healthy cohort'


# ============================================================================
# CORE CLASSIFICATION
# ============================================================================

def find_disease(text):
    """Return (label, pattern) for the first matching disease pattern, else (None, None)."""
    for rx, lab in DISEASE_RE:
        if rx.search(text):
            return lab, rx.pattern
    return None, None


def classify_row(std, ca):
    """Classify one sample.

    Parameters
    ----------
    std : dict   standard checklist fields for this row (raw values)
    ca  : dict   parsed custom_attributes JSON for this row

    Returns
    -------
    (disease, is_tumor, is_control, from_sample, evidence) where is_tumor and
    is_control are True/False/None and from_sample marks a sample-derived disease.
    """
    merged = {}
    merged.update({k: str(v).strip().lower() for k, v in std.items() if v})
    merged.update({k.lower(): str(v).strip().lower() for k, v in ca.items()})

    evidence = []

    # ---------------- TUMOR ----------------
    is_tumor = None
    for k in ('is_tumor', 'tumor', 'tumour'):           # explicit boolean key wins
        if k in merged:
            v = merged[k]
            if v in ('yes', 'true', '1', 'tumor', 'tumour', 't'):
                is_tumor = True; evidence.append(f'{k}={v}'); break
            if v in ('no', 'false', '0', 'normal', 'control', 'n'):
                is_tumor = False; evidence.append(f'{k}={v}'); break
    if is_tumor is None:
        for f in TUMOR_FIELDS:
            if f in merged:
                v = merged[f]
                if TUMOR_NEG_RE.search(v):
                    is_tumor = False; evidence.append(f'{f}~={v[:30]}(non-tumor)'); break
                if TUMOR_POS_RE.search(v) or v in ('tumor', 'tumour', 'cancer', 'crc tumor'):
                    is_tumor = True; evidence.append(f'{f}~={v[:30]}(tumor)'); break

    # ---------------- CONTROL ----------------
    is_control = None
    for f in CONTROL_FIELDS:
        if f in merged:
            v = merged[f]
            if not v:
                continue
            if CONTROL_RE.search(v):
                is_control = True; evidence.append(f'{f}~={v[:30]}(control)'); break
            if v in CONTROL_VALUES:
                if v in ('no', 'negative') and f not in ('case_control', 'tumor', 'is_tumor'):
                    continue
                is_control = True; evidence.append(f'{f}={v[:30]}(control)'); break
    if 'subset_healthy' in merged and merged['subset_healthy'] in ('true', 'yes', '1'):
        if is_control is None:
            is_control = True; evidence.append('subset_healthy=true')
    if is_control is None:                              # explicit healthy disease-status
        for f in ('host_disease', 'host_disease_status', 'disease', 'diagnosis',
                  'clinical_condition', 'condition', 'disease_state'):
            if f in merged and merged[f] in HEALTHY_STATUS:
                is_control = True; evidence.append(f'{f}={merged[f][:20]}(healthy)'); break
    if is_tumor is True:                                # a tumor is not a control
        is_control = False

    # ---------------- DISEASE (sample level) ----------------
    disease = None; dis_ev = None; from_sample = False
    for f in DISEASE_FIELDS_DEDICATED:
        if f not in merged:
            continue
        v = merged[f]
        if v in NULLISH:
            continue
        if v in ABBREV_MAP:                             # exact-value abbreviation
            disease = ABBREV_MAP[v]; dis_ev = f'{f}={v}'; from_sample = True; break
        # pathogen test result: "<x> negative" = control (not infected);
        # "<x> positive" = infection (case). Don't treat the result as a disease.
        pn = None
        if re.search(r'\b(negative|not detected|undetected|absent|none detected)\b', v):
            pn = 'neg'
        elif re.search(r'\b(positive|detected|present)\b', v):
            pn = 'pos'
        if pn:
            patho = re.sub(r'\b(positive|negative|not detected|undetected|absent|none detected|'
                           r'detected|present|test|result|status|for|infection|pcr)\b', '', v)
            patho = patho.strip(' ,:-/')
            lab_p, _ = find_disease(patho) if patho else (None, None)
            topic = lab_p or ((patho + ' infection')
                              if (patho and patho not in NULLISH and len(patho) > 1) else None)
            if pn == 'neg':
                if is_control is None:
                    is_control = True; evidence.append(f'{f}={v[:25]}(neg->control)')
                if topic:
                    disease = topic; dis_ev = f'{f}={v[:25]}(study topic)'  # from_sample stays False
                break
            disease = topic or 'infection'; dis_ev = f'{f}={v[:25]}(pos)'; from_sample = True
            break
        lab, _ = find_disease(v)
        if lab:
            disease = lab; dis_ev = f'{f}={v[:30]}'; from_sample = True; break
        if f in DISEASE_VERBATIM_OK and len(v) > 2 and not v.replace('.', '').isdigit():
            disease = v; dis_ev = f'{f}={v[:30]}(verbatim)'; from_sample = True; break
    if disease is None:                                 # keyword scan over free text
        for f in ['title', 'isolation_source', 'tissue_type', 'scientific_name', 'description',
                  'group', 'site', 'sample_type', 'cell_line']:
            if f in merged and merged[f] not in NULLISH:
                lab, _ = find_disease(merged[f])
                if lab:
                    disease = lab; dis_ev = f'{f}~={merged[f][:30]}'; from_sample = True; break
    if disease:
        evidence.append(f'disease<-{dis_ev}')

    # ---------------- tumor => cancer ----------------
    if is_tumor is True:
        if disease not in CANCER_LABELS:
            disease = disease if disease in CANCER_LABELS else 'cancer'
            from_sample = True
        evidence.append('tumor=>cancer')

    # ---------------- case assignment ----------------
    if is_control is None and from_sample and disease and disease != NO_DISEASE:
        is_control = False

    return disease, is_tumor, is_control, from_sample, '; '.join(evidence)


# ============================================================================
# PASS 1 -- learn each study's dominant disease (gated)
# ============================================================================

def parse_ca(value):
    if value is None or (isinstance(value, float)) or pd.isna(value):
        return {}
    try:
        d = json.loads(value)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def learn_study_diseases(vote_files, chunksize, log):
    """Scan vote_files and return {study: dominant_disease} for disease-focused studies."""
    votes = defaultdict(Counter)
    study_size = Counter()
    study_case = Counter()
    usecols = ['accession', 'source_study', 'custom_attributes'] + STD_TEXT_FIELDS

    for fn in vote_files:
        for chunk in pd.read_csv(fn, usecols=lambda c: c in usecols, chunksize=chunksize, dtype=str):
            cols = chunk.columns
            for _, r in chunk.iterrows():
                std = {c: r[c] for c in STD_TEXT_FIELDS if c in cols and pd.notna(r[c])}
                ca = parse_ca(r.get('custom_attributes'))
                disease, _, is_control, from_sample, _ = classify_row(std, ca)
                study = r['source_study']
                study_size[study] += 1
                if from_sample and disease and is_control is False and disease != NO_DISEASE:
                    votes[study][disease] += 1
                    study_case[study] += 1
        log(f'  voted over {os.path.basename(fn)}')

    study_disease = {}
    for s, ctr in votes.items():
        n_case = study_case[s]
        n_total = study_size[s]
        case_frac = n_case / n_total if n_total else 0
        most = ctr.most_common()
        top_lab, top_n = most[0]
        top_share = top_n / n_case if n_case else 0
        n_distinct = len(most)
        # prefer a specific disease over generic 'cancer' when well supported
        if top_lab == 'cancer' and len(most) > 1 and most[1][1] >= max(3, 0.2 * top_n):
            top_lab = most[1][0]
        # GATE: only propagate for a focused case/control study -- a single
        # disease dominates AND it isn't a mixed-condition population cohort.
        focused = (top_share >= 0.55 and n_distinct <= 4)
        if focused and ((case_frac >= 0.15) or (n_total <= 300 and case_frac >= 0.30)):
            study_disease[s] = top_lab
    return study_disease, dict(study_size)


# ============================================================================
# PASS 2 -- write augmented output
# ============================================================================

def finalize(std, ca, study, study_disease):
    disease, is_tumor, is_control, _, ev = classify_row(std, ca)
    final = disease
    sd = study_disease.get(study)
    if not final:
        if sd:
            final = sd
            ev = (ev + '; ' if ev else '') + f'disease<-study_vote({sd})'
        else:
            final = NO_DISEASE
    if is_tumor is True and final not in CANCER_LABELS:
        final = final if final in CANCER_LABELS else 'cancer'
    yn = {True: 'yes', False: 'no', None: 'unknown'}
    return final, yn[is_control], yn[is_tumor], ev


def classify_chunk(chunk, study_disease):
    cols = list(chunk.columns)
    dis, ctl, tum, evd = [], [], [], []
    for _, r in chunk.iterrows():
        std = {c: r[c] for c in STD_TEXT_FIELDS if c in cols and pd.notna(r[c])}
        ca = parse_ca(r.get('custom_attributes'))
        d, c, t, e = finalize(std, ca, r['source_study'], study_disease)
        dis.append(d); ctl.append(c); tum.append(t); evd.append(e)
    chunk = chunk.copy()
    chunk['disease'] = dis
    chunk['is_control'] = ctl
    chunk['is_tumor'] = tum
    chunk['classification_evidence'] = evd
    return chunk[cols + ['disease', 'is_control', 'is_tumor', 'classification_evidence']]


def write_combined(input_files, study_disease, out_path, compress, chunksize, log):
    """Write one combined CSV (optionally gzip/zip compressed). Returns aggregate counters."""
    agg = {'disease': Counter(), 'is_control': Counter(), 'is_tumor': Counter()}
    n = 0
    parent = os.path.dirname(os.path.abspath(out_path))
    os.makedirs(parent, exist_ok=True)
    # always build an uncompressed/temp text stream first, then compress on close
    plain_path = out_path
    if compress == 'gzip' and not plain_path.endswith('.gz'):
        plain_path = out_path + '.gz'
    if compress == 'zip':
        plain_path = out_path[:-4] if out_path.endswith('.zip') else out_path  # inner csv name

    if compress == 'gzip':
        fh = gzip.open(plain_path, 'wt', newline='')
    else:
        fh = open(plain_path, 'w', newline='')

    header_written = False
    try:
        for fn in input_files:
            for chunk in pd.read_csv(fn, chunksize=chunksize, dtype=str):
                out = classify_chunk(chunk, study_disease)
                out.to_csv(fh, header=not header_written, index=False)
                header_written = True
                n += len(out)
                for col in agg:
                    agg[col].update(out[col])
            log(f'  classified {os.path.basename(fn)}')
    finally:
        fh.close()

    if compress == 'zip':
        zip_path = out_path if out_path.endswith('.zip') else out_path + '.zip'
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(plain_path, arcname=os.path.basename(plain_path))
        os.remove(plain_path)
        final_path = zip_path
    elif compress == 'gzip':
        final_path = plain_path
    else:
        final_path = plain_path
    return n, agg, final_path


def write_per_file(input_files, study_disease, out_dir, compress, chunksize, log):
    os.makedirs(out_dir, exist_ok=True)
    agg = {'disease': Counter(), 'is_control': Counter(), 'is_tumor': Counter()}
    n = 0
    written = []
    for fn in input_files:
        base = os.path.basename(fn)
        stem = base[:-4] if base.lower().endswith('.csv') else base
        out_csv = os.path.join(out_dir, stem + '_classified.csv')
        target = out_csv + '.gz' if compress == 'gzip' else out_csv
        fh = gzip.open(target, 'wt', newline='') if compress == 'gzip' else open(target, 'w', newline='')
        header_written = False
        try:
            for chunk in pd.read_csv(fn, chunksize=chunksize, dtype=str):
                out = classify_chunk(chunk, study_disease)
                out.to_csv(fh, header=not header_written, index=False)
                header_written = True
                n += len(out)
                for col in agg:
                    agg[col].update(out[col])
        finally:
            fh.close()
        if compress == 'zip':
            zpath = out_csv + '.zip'
            with zipfile.ZipFile(zpath, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.write(out_csv, arcname=os.path.basename(out_csv))
            os.remove(out_csv)
            target = zpath
        written.append(target)
        log(f'  wrote {target}')
    return n, agg, written


# ============================================================================
# CLI
# ============================================================================

def expand(patterns):
    files = []
    for p in patterns:
        hits = sorted(glob.glob(p))
        files.extend(hits if hits else ([p] if os.path.exists(p) else []))
    # de-dup, keep order
    seen, out = set(), []
    for f in files:
        if f not in seen:
            seen.add(f); out.append(f)
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(
        description='Classify ENA sample metadata (disease / control / tumor).',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('inputs', nargs='+',
                    help='Input CSV file(s) or glob(s) to classify and write out.')
    ap.add_argument('-o', '--output', required=True,
                    help='Output file (combined mode) or directory (per-file mode).')
    ap.add_argument('--mode', choices=['combined', 'per-file'], default='combined',
                    help='Write one combined file (default) or one file per input.')
    ap.add_argument('--compress', choices=['none', 'gzip', 'zip'], default='none',
                    help='Compress the output. Recommended for large outputs.')
    ap.add_argument('--vote-scope', nargs='+', default=None,
                    help='File(s)/glob(s) over which to compute study-level disease '
                         'votes. Defaults to INPUTS. Set this to the whole dataset '
                         'when classifying only a subset, so study labels stay '
                         'consistent across batches.')
    ap.add_argument('--chunksize', type=int, default=20000,
                    help='Rows per read chunk (memory/throughput trade-off).')
    ap.add_argument('-q', '--quiet', action='store_true', help='Suppress progress logging.')
    args = ap.parse_args(argv)

    def log(msg):
        if not args.quiet:
            print(msg, file=sys.stderr)

    input_files = expand(args.inputs)
    if not input_files:
        ap.error('no input files matched')
    vote_files = expand(args.vote_scope) if args.vote_scope else input_files

    log(f'Inputs to classify : {len(input_files)} file(s)')
    log(f'Vote scope         : {len(vote_files)} file(s)')
    log('Pass 1: learning study-level diseases ...')
    study_disease, _ = learn_study_diseases(vote_files, args.chunksize, log)
    log(f'  {len(study_disease)} studies qualify as disease-focused')

    log('Pass 2: writing classified output ...')
    if args.mode == 'combined':
        n, agg, final_path = write_combined(
            input_files, study_disease, args.output, args.compress, args.chunksize, log)
        outputs = [final_path]
    else:
        n, agg, outputs = write_per_file(
            input_files, study_disease, args.output, args.compress, args.chunksize, log)

    log(f'\nDone. {n:,} rows written.')
    log(f'  is_control : {dict(agg["is_control"])}')
    log(f'  is_tumor   : {dict(agg["is_tumor"])}')
    log('  top diseases:')
    for k, v in agg['disease'].most_common(12):
        log(f'     {v:>8}  {k}')
    log('  output:')
    for o in outputs:
        log(f'     {o}')


if __name__ == '__main__':
    main()
