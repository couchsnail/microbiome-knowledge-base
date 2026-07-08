"""where main API script will be run
   needs to be able to expand for different databases later
   The current code is an example and right now only intended to work with ENA
   as a proof of concept. 

   TO RUN: 
   - PYTHONPATH=. fastapi dev api/main.py (it can't find scripts otherwise for the imports)

   TO-DO:
   - Get basic code to work (DONE)
   - Add in ENA Portal: https://www.ebi.ac.uk/ena/portal/api/swagger-ui/index.html (DONE)
   - Integrate with Cham's script 
     - Fetching samples: Yes 
     - Classifying samples: Yesn't
   - Integrate with PostGreSQL for large database size
   - Integrate with pre-existing HTML webpage

   """
from fastapi import FastAPI
from ena_accessor import fetch
from scripts.fetch_ena_samples import run # note: immensely janky right now

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str | None = None):
    return {"item_id": item_id, "q": q}

"""Used for accession code fetching using the public ENA API"""
@app.get("/fetch/{accession}")
def fetch_accession(accession: str):
    run(accession)
    # return data
    """data = fetch(accession)
    return data"""

"""Used for parsing data and then adding it to csv/database"""
@app.post("/submit")
def submit(accession: str):
    run(accession=[accession])

# Debugger
if __name__ == '__main__':
    print("Pain and Suffering")

"""Anyway from my understanding is that I have to get some code that like
I dunno
upon the user saying 'Hey so I want this like parsed or whatever', send a message to...
command line
hmmm"""
