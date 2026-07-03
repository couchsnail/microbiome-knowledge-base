"""This file is for fetching data from the ENA API"""
import requests

"""Basically this one is like, 'So get the given study with the API'"""
def fetch(accession):
    url = f'https://www.ebi.ac.uk/ena/portal/api/filereport?result=read_run&fields=fastq_ftp&format=json&accession={accession}'
    response = requests.get(url)
    return response.json()