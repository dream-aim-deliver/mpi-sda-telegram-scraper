import os
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import Field
from typing import List

from app.telegram_scraper_impl import TelegramScraperJob, TelegramScraperJobManager

load_dotenv()


API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
USERNAME = os.getenv("USERNAME")
HOST = os.getenv("HOST", "localhost")
PORT = int(os.getenv("PORT", "8000"))
MODE = os.getenv("MODE", "production")

app = FastAPI()
app.job_manager = TelegramScraperJobManager() # type: ignore

    
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

# class TelegramScrapeRequest(BaseModel):
#     channel: str = "GCC_report"

# @app.put("/telegram/scrape")
# async def scrape_telegram(request: TelegramScrapeRequest):
#     datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     outfile = f"data_telegram_{request.channel}_{datetime}.csv"

#     print(f"{datetime} - Starting scraping {outfile}")
#     start_time = time.time()
#     # This should be a background task
#     # await scrape(request.channel, outfile)
#     print(f"{datetime} - Scraping completed in {time.time() - start_time} seconds")
    
#     return {"data": "Scraping completed"}
