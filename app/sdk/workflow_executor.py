from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
import logging
from typing import List
from fastapi import BackgroundTasks

from pydantic import BaseModel
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway
from app.sdk.minio_repository import MinIORepository
from app.sdk.models import LFN


logger = logging.getLogger(__name__)


class BaseWorkflowStatus(Enum):
    CREATED = "created"
    RUNNING = "running"
    ERROR = "error"
    FINISHED = "finished"


class WorkfowArgs(BaseModel):
    id: int
    created_at: datetime = datetime.now()
    heartbeat: datetime = datetime.now()
    name: str
    args: dict = {}
    state: Enum = BaseWorkflowStatus.CREATED
    messages: List[str] = []
    output_lfns: List[LFN] = []
    input_lfns: List[LFN] = []


class BaseWorkflowExecutor(BackgroundTasks):
    def __init__(
        self,
        tracer_key: str,
        kp_host: str,
        kp_port: str,
        minio_host: str,
        minio_port: str,
        minio_bucket: str,
        minio_access_key: str,
        minio_secret_key: str,
    ) -> None:
        self._tracer_key = tracer_key
        self._minio_repository = MinIORepository(
            host=minio_host,
            port=minio_port,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            bucket=minio_bucket,
        )
        self._kernel_plankster_gateway = KernelPlancksterGateway(
            host=kp_host, port=kp_port
        )

    @property
    def minio_repository(self) -> MinIORepository:
        return self._minio_repository

    @property
    def kernel_plankster_gateway(self) -> KernelPlancksterGateway:
        return self._kernel_plankster_gateway

    @property
    def tracer_key(self) -> str:
        return self._tracer_key

    @abstractmethod
    async def run(self, *args, **kwargs):
        raise NotImplementedError("run method must be implemented for your workflow!!")

    def execute(self, *args, **kwargs):
        self.add_task(self.run)
