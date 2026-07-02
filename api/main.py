"""where main API script will be run
   needs to be able to expand for different databases later
   The current code is an example and right now only intended to work with ENA
   as a proof of concept. 

   TO RUN: 
   - cd api -> fastapi dev

   TO-DO:
   - Get basic code to work
   - Add in ENA Portal: https://www.ebi.ac.uk/ena/portal/api/swagger-ui/index.html
   - Integrate with Cham's script (?) (but it only need for like, one row)
   - Integrate with PostGreSQL for large database size
   - Integrate with pre-existing HTML webpage

   """
from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str | None = None):
    return {"item_id": item_id, "q": q}