import os
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import Field
from typing import List
from app.sdk.job_router import JobManagerFastAPIRouter

from telegram_scraper import scrape
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


HOST = os.getenv("HOST", "localhost")
PORT = int(os.getenv("PORT", "8000"))
MODE = os.getenv("MODE", "production")

app = FastAPI()
app.job_manager = TelegramScraperJobManager()  # type: ignore

job_manager_router = JobManagerFastAPIRouter(app, scrape)
