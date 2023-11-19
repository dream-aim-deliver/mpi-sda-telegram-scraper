import random
from typing import Dict, Generic, TypeVar
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel


class BaseJob(BaseModel):
    id: int
    workflow: str
    job_args: Dict[str, str] = {}


class BaseWorkflow:
    def __init__(self, name: str, app: FastAPI):
        self.name = name
        self.app = app
        self.router = APIRouter(prefix=f"/{self.name}")
        self.create_job_route()
        self.app.include_router(self.router)

    def create_job(self, *args, **kwargs) -> BaseJob:
        return BaseJob(id=random.randint(0, 1000), workflow=self.name, job_args=kwargs)

    def create_job_route(self):
        @self.router.post(
            f"/",
            name="Create a new Workflow Execution Request",
            response_model=BaseJob,
        )
        def parent_wrapper(parent: int):
            return self.create_job()


class DataProcessingJob(BaseJob):
    telegram_channel: str


class DataProcessingWorkflow(BaseWorkflow):
    def __init__(self, app: FastAPI):
        super().__init__("data_processing", app)

    def create_job_route(self):
        @self.router.post(
            f"/",
            name="Create a new Data Processing Workflow Execution Request",
            response_model=BaseJob,
        )
        def child_wrapper(telegram_channel: str):
            job: BaseJob = super(DataProcessingWorkflow, self).create_job(
                telegram_channel=telegram_channel
            )
            return job


app = FastAPI()
DataProcessingWorkflow(app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("test_server:app", host="localhost", port=8000, reload=True)
