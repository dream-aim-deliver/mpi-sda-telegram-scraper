import os
from dotenv import load_dotenv
from fastapi import FastAPI
from app.data_preparation_workflow import DataPreparationWorkflowManager
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway
from app.sdk.minio_repository import MinIORepository

from telegram_scraper import scrape
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    MINIO_HOST = os.getenv("MINIO_HOST", "localhost")
    MINIO_PORT = os.getenv("MINIO_PORT", "9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")

    KERNEL_PLANCKSTER_HOST = os.getenv("KERNEL_PLANCKSTER_HOST", "http://localhost")
    KERNEL_PLANCKSTER_PORT = os.getenv("KERNEL_PLANCKSTER_PORT", "8000")

    minio = MinIORepository(
        host=MINIO_HOST,
        port=MINIO_PORT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
    )

    kernel_planckster_gateway = KernelPlancksterGateway(
        host=KERNEL_PLANCKSTER_HOST, port=KERNEL_PLANCKSTER_PORT
    )

    app = FastAPI(
        title="Lieutenant Planckster",
        version="1.0.0-alpha",
        description="Startlette/FastAPI Workflow Management System",
    )
    app.kernel_plankster = kernel_planckster_gateway  # type: ignore
    app.minio_repository = minio  # type: ignore
    app.workflows = {}  # type: ignore
    return app


HOST = os.getenv("HOST", "localhost")
PORT = int(os.getenv("PORT", "8000"))
MODE = os.getenv("MODE", "production")
app = create_app()

# TIP: Here you register your workflow managers into the app
DataPreparationWorkflowManager(app)


if __name__ == "__main__":
    import uvicorn

    HOST = os.getenv("HOST", "localhost")
    PORT = int(os.getenv("PORT", "8000"))
    MODE = os.getenv("MODE", "production")
    print(f"Starting server on {HOST}:{PORT}")
    uvicorn.run("lieutenant_planckster:app", host=HOST, port=PORT, reload=True)
