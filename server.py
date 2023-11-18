import time
import os
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel, Field
from typing import List
from datetime import datetime

from app.sdk.models import BaseJob

load_dotenv()


API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
USERNAME = os.getenv("USERNAME")
HOST = os.getenv("HOST", "localhost")
PORT = int(os.getenv("PORT", "8000"))
MODE = os.getenv("MODE", "production")



class CreateJobResponse(BaseJob):
    output_lfn: List[str] = Field(descipton="Output LFNs")


app = FastAPI()
JOBS: List[BaseJob] = [BaseJob(id=0, name="test", output_lfn=["test"], input_lfns=["test"], status="created")]
JOB_NONCE = 0

class TelegramScrapeRequest(BaseModel):
    channel: str = "GCC_report"
    
@app.get("/job")
def list_all_jobs() -> List[BaseJob]:
    return JOBS


@app.post("/job")
def create_job() -> BaseJob:
    global JOB_NONCE
    JOB_NONCE += 1  
    date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    name = f"telegram-{JOB_NONCE}-{date}"
    output_lfns = [f"/telegram/{JOB_NONCE}/{date}/data2_climate.csv"]
    job = BaseJob(
        id=JOB_NONCE,
        name=name,
        output_lfn=output_lfns,
    )
    JOBS.append(job)
    return job

@app.put("/telegram/scrape")
async def scrape_telegram(request: TelegramScrapeRequest):
    datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    outfile = f"data_telegram_{request.channel}_{datetime}.csv"

    print(f"{datetime} - Starting scraping {outfile}")
    start_time = time.time()
    # This should be a background task
    # await scrape(request.channel, outfile)
    print(f"{datetime} - Scraping completed in {time.time() - start_time} seconds")
    
    return {"data": "Scraping completed"}
