from typing import List
from fastapi import APIRouter

from app.sdk.models import LFN


class JobManagerFastAPIRouter:
    def __init__(self, app):
        self.app = app
        self.router = APIRouter()
        self.register_endpoints()
        self.app.include_router(self.router)

    def register_endpoints(self):
        @self.router.get("/job")
        def list_all_jobs():
            job_manager = self.app.job_manager  # type: ignore
            return job_manager.list_jobs()

        @self.router.post("/job")
        def create_job(tracer_id: str, input_lfns: List[LFN] | None = None):
            job_manager = self.app.job_manager  # type: ignore
            job: TelegramScraperJob = job_manager.create_job(tracer_id)  # type: ignore
            return job

        @self.router.get("/job/{job_id}")
        def get_job(job_id: int):
            job_manager = self.app.job_manager  # type: ignore
            job = job_manager.get_job(job_id)
            return job
