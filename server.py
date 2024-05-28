import os
from dotenv import load_dotenv
from fastapi import FastAPI
from app.sdk.job_manager import BaseJobManager
from app.sdk.job_router import JobManagerFastAPIRouter

from telegram_scraper import scrape
import logging
import argparse

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


def create_app():  # created a function for better modularity
    app = FastAPI()
    app.job_manager = BaseJobManager()  # type: ignore
    job_manager_router = JobManagerFastAPIRouter(app, scrape)
    return app


if __name__ == "__main__":
    import uvicorn

    parser = argparse.ArgumentParser(description="Start the server")

    parser.add_argument(
        "--host",
        default="localhost",
        help="The host to run the server on",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="The port to run the server on",
    )

    parser.add_argument(
        "--mode",
        default="production",
        help="The mode to run the server in (e.g., production, development)",
    )

    args = parser.parse_args()

    HOST = args.host
    PORT = args.port
    MODE = args.mode

    app = create_app()

    print(f"Starting server on {HOST}:{PORT} in {MODE} mode")
    uvicorn.run("server:app", host=HOST, port=PORT, reload=True)
