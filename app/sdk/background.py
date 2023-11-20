from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, TypeVar
from typing_extensions import ParamSpec
from fastapi import BackgroundTasks as FastAPIBackgroundTasks
from pydantic import BaseModel, Field

from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway
from app.sdk.minio_repository import MinIORepository
from app.sdk.models import BaseJob

P = ParamSpec("P")


class WorkerArgs(BaseModel):
    tracer_id: str = Field(
        description="A unique identifier to trace jobs across the SDA runtime."
    )
    minio_repository: MinIORepository
    kernel_plankster_gateway: KernelPlancksterGateway


TWorkerArgs = TypeVar("TWorkerArgs", bound=WorkerArgs)


class BackgroundWorker(ABC, FastAPIBackgroundTasks, Generic[TWorkerArgs]):
    def __init__(
        self,
        tracer_key: str,
        kp_gateway: KernelPlancksterGateway,
        minio_repository: MinIORepository,
    ):
        super().__init__()
        self._tracer_key = tracer_key
        self._kernel_plankster_gateway = kp_gateway
        self._minio_repository = minio_repository

    @property
    def tracer_key(self) -> str:
        return self._tracer_key

    @property
    def kernel_plankster_gateway(self) -> KernelPlancksterGateway:
        return self._kernel_plankster_gateway

    @property
    def minio_repository(self) -> MinIORepository:
        return self._minio_repository

    @abstractmethod
    def run(
        self,
        *args,
    ) -> None:
        raise NotImplementedError("run method must be implemented for your worker!!")
