import random
from typing import Dict
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel


class BaseJob(BaseModel):
    id: int
    workflow: str
    job_args: Dict[str, str] = {}


class BaseWorkflowManager:
    def __init__(self, name: str, app: FastAPI):
        self._name = name
        self._app = app
        self._router = APIRouter(prefix=f"/{self._name}")
        self._app.include_router(self._router)
        self.create_job_route()

    @property
    def name(self) -> str:
        return self._name

    @property
    def router(self) -> APIRouter:
        return self._router

    def create_job(self, *args, **kwargs) -> BaseJob:
        return BaseJob(id=random.randint(0, 1000), workflow=self._name, job_args=kwargs)

    def create_job_route(self):
        @self._router.post(
            f"/",
            name="Create a new Workflow Execution Request",
            response_model=BaseJob,
        )
        def request_workflow_run(parent: int):
            return self.create_job()

    def list_jobs_route(self):
        @self._router.get(
            f"/",
            name="List all Workflow Execution Requests",
            response_model=BaseJob,
        )
        def list_workflow_runs():
            return self.create_job()


class DataProcessingJob(BaseJob):
    telegram_channel: str


class DataProcessingWorkflowManager(BaseWorkflowManager):
    def __init__(self, app: FastAPI):
        super().__init__("data_processing", app)

    def create_job_route(self):
        @self._router.post(
            f"/",
            name="Create a new Data Processing Workflow Execution Request",
            response_model=BaseJob,
        )
        def request_workflow_run(telegram_channel: str):
            job: BaseJob = super(DataProcessingWorkflowManager, self).create_job(
                telegram_channel=telegram_channel
            )
            return job


app = FastAPI()
DataProcessingWorkflowManager(app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("test_server:app", host="localhost", port=8000, reload=True)
