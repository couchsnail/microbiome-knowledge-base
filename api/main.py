"""where main API script will be run
   needs to be able to expand for different databases later
   The current code is an example and right now only intended to work with ENA
   as a proof of concept. 

   TO RUN: 
   - PYTHONPATH=. fastapi dev api/main.py (it can't find scripts otherwise for the imports)
   - python fetch_ena_samples.py --accession-codes PRJEB8073 
     - for debugging purposes
   - If you're running this locally, you need to create a .env folder with
    - DB_URL=postgresql://postgres:password#@localhost:5432/database
    - Otherwise it's a security risk
   - Need postgresql installed (and pgadmin4 but this won't work if you're on Mac)
   - Need to install sqlalchemy
   - Need to install python-dotenv (security reasons)
   - 

   TO-DO:
   - Get basic code to work (DONE)
   - Add in ENA Portal: https://www.ebi.ac.uk/ena/portal/api/swagger-ui/index.html (DONE)
   - Integrate with Camille's script 
     - Fetching samples: Yes (except not really freaking HTML)
     - Classifying samples: Yesn't (not really sure if this is strictly necessary)
   - Integrate with PostGreSQL for large database size (DONE)
   - Integrate with pre-existing HTML webpage (DONE)
   - Add NCBI accessor codes if needed later on 
   - Can be used in a paper (so clean up thoroughly)

   Documentation for FastAPI with JSON: https://fastapi.tiangolo.com/tutorial/body/
   Documentation for getting around CORS: https://fastapi.tiangolo.com/tutorial/cors/#wildcards

   """
from fastapi import FastAPI
from ena_accessor import fetch
from scripts.fetch_ena_samples import run, addToDatabase, retrieveDatabase, downloadCSV# note: immensely janky right now
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI()

# Default base model
@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str | None = None):
    return {"item_id": item_id, "q": q}

# Set-up so that you don't get error messages from it being sent to the wrong place
"""origins = [
    "http://localhost",
    "https://127.0.0.1",
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:55581"
]"""

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # replace later with more specific ones
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Holdovers from when I was debugging; may need to use these later
class AccessionCode(BaseModel):
    accession_code: str

class DataFrame(BaseModel):
    dataframe: list[dict]

"""
Used for accession code fetching using the public ENA API.
Calls the run function from fetch_ena_samples to process and find the studies, then
Filters returns them. 
"""
@app.get("/fetch/{accession}")
def fetch_accession(accession: str):
    df = run(accession_codes=accession) 
    df = df.astype(object).where(pd.notnull(df), None)
    print("Returning df to user")
    return {"status": "ok", "accession": accession, "data": df.to_dict(orient="records")}
    # return data

"""
Used for parsing data and then adding it to csv/database.
Calls addToDatabase() from fetch_ena_samples
"""
# Status Code documentation: https://fastapi.tiangolo.com/advanced/response-change-status-code/
# Sequence should be: Press "Upload to database" => Call run(accession) => do all that fun stuff => Write to database and not a csv
@app.post("/submit")
def submit(data: DataFrame):
    df = pd.DataFrame(data.dataframe)
    addToDatabase(df)
    # tsv_data = createTSV()
    return {"status": "ok"}

"""
Used for downloading information as CSV files.
"""
@app.post("/download")
def download(data: DataFrame):
    df = pd.DataFrame(data.dataframe)
    downloadCSV(df)
    return {"status": "ok"}


"""
Used for pulling up database information via call to 
retrieveDatabase() from fetch_ena_samples.
"""
@app.get("/database")
def get_database():
    results = retrieveDatabase()
    return {"status": "ok", "data": results}

# Debugger
if __name__ == '__main__':
    print("Debugging")