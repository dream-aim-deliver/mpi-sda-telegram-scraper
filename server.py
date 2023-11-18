import os
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import Field
from typing import List
from app.sdk.models import LFN, BaseJobState, DataSource, Protocol

from app.telegram_scraper_impl import TelegramScraperJob, TelegramScraperJobManager
from telegram_scraper import scrape
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO
                    , format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                    , datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
USERNAME = os.getenv("USERNAME")
HOST = os.getenv("HOST", "localhost")
PORT = int(os.getenv("PORT", "8000"))
MODE = os.getenv("MODE", "production")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")


app = FastAPI()
app.job_manager = TelegramScraperJobManager() # type: ignore

data_dir = os.path.join(os.path.dirname(__file__), "data")
    
@app.get("/job")
def list_all_jobs() -> List[TelegramScraperJob]:
    job_manager: TelegramScraperJobManager = app.job_manager # type: ignore
    return job_manager.list_jobs()

@app.post("/job")
def create_job(tracer_id: str) -> TelegramScraperJob:
    job_manager: TelegramScraperJobManager = app.job_manager # type: ignore
    job: TelegramScraperJob = job_manager.create_job(tracer_id) # type: ignore
    return job

@app.get("/job/{job_id}")
def get_job(job_id: int) -> TelegramScraperJob:
    job_manager: TelegramScraperJobManager = app.job_manager # type: ignore
    job = job_manager.get_job(job_id)
    return job

@app.post("/job/{job_id}/start")
def start_job(job_id: int, background_tasks: BackgroundTasks):
    job_manager: TelegramScraperJobManager = app.job_manager # type: ignore
    job = job_manager.get_job(job_id)
    if API_ID is None or API_HASH is None:
        job.state = BaseJobState.FAILED
        job.messages.append("Status: FAILED. API_ID and API_HASH must be set. ")
        raise HTTPException(status_code=500, detail="API_ID and API_HASH must be set.")
    background_tasks.add_task(scrape, job=job, channel_name="GCC_report", api_id=API_ID, api_hash=API_HASH)

@app.get("/lfn")
def create_lfn() -> LFN:
    lfn = LFN(
        protocol=Protocol.LOCAL,
        tracer_id="test",
        job_id=20,
        source=DataSource.TELEGRAM,
        relative_path="test.csv",
    )
    return lfn
