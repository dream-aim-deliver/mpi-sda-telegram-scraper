import os
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import Field
from typing import List
from app.sdk.job_router import JobManagerFastAPIRouter
from app.sdk.minio_gateway import MinIORepository
from app.sdk.models import LFN, BaseJobState, DataSource, Protocol

from app.telegram_scraper_impl import TelegramScraperJob, TelegramScraperJobManager
from telegram_scraper import scrape
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
USERNAME = os.getenv("USERNAME")
HOST = os.getenv("HOST", "localhost")
PORT = int(os.getenv("PORT", "8000"))
MODE = os.getenv("MODE", "production")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_HOST = os.getenv("MINIO_HOST", "localhost")
MINIO_PORT = os.getenv("MINIO_PORT", "9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "mpi-sda-telegram-scraper")
STORAGE_PROTOCOL_CONFIG = os.getenv("STORAGE_PROTOCOL", "S3")
STORAGE_PROTOCOL = Protocol(STORAGE_PROTOCOL_CONFIG.lower())

if not (STORAGE_PROTOCOL == Protocol.S3 or STORAGE_PROTOCOL == Protocol.LOCAL):
    raise ValueError(
        f"Invalid STORAGE_PROTOCOL: {STORAGE_PROTOCOL_CONFIG}. "
        f"Valid values are: {Protocol.S3.value}, {Protocol.LOCAL.value}"
    )

app = FastAPI()
app.job_manager = TelegramScraperJobManager()  # type: ignore

job_manager_router = JobManagerFastAPIRouter(app)

data_dir = os.path.join(os.path.dirname(__file__), "data")


@app.post("/job/{job_id}/start")
def start_job(job_id: int, background_tasks: BackgroundTasks):
    job_manager: TelegramScraperJobManager = app.job_manager  # type: ignore
    job = job_manager.get_job(job_id)
    minio_repository = MinIORepository(
        host=MINIO_HOST,
        port=MINIO_PORT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket=MINIO_BUCKET,
    )
    try:
        minio_repository.create_bucket_if_not_exists(MINIO_BUCKET)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to MinIO: {e}")

    if API_ID is None or API_HASH is None:
        job.state = BaseJobState.FAILED
        job.messages.append("Status: FAILED. API_ID and API_HASH must be set. ")
        raise HTTPException(status_code=500, detail="API_ID and API_HASH must be set.")
    background_tasks.add_task(
        scrape,
        job=job,
        channel_name="GCC_report",
        api_id=API_ID,
        api_hash=API_HASH,
        minio_repository=minio_repository,
    )
