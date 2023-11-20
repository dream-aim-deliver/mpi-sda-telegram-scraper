from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
import logging
from typing import Any, List
from fastapi import BackgroundTasks

from pydantic import BaseModel
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway
from app.sdk.minio_repository import MinIORepository
from app.sdk.models import LFN
from app.sdk.workflow_manager import BaseWorkflow


logger = logging.getLogger(__name__)


class BaseWorkflowExecutor:
    def __init__(
        self,
        workflow: BaseWorkflow,
        minio: MinIORepository,
        kernel_plankster: KernelPlancksterGateway,
    ) -> None:
        super().__init__()
        self._workflow = workflow
        self._minio_repository = minio
        self._kernel_plankster_gateway = kernel_plankster

    @property
    def minio_repository(self) -> MinIORepository:
        return self._minio_repository

    @property
    def messages(self) -> List[str]:
        return self._workflow.messages

    @property
    def kernel_plankster_gateway(self) -> KernelPlancksterGateway:
        return self._kernel_plankster_gateway

    @property
    def minio(self) -> MinIORepository:
        return self._minio_repository

    @property
    def workflow(self) -> BaseWorkflow:
        return self._workflow

    @property
    def tracer_key(self) -> str:
        return self._workflow.tracer_key

    async def run(self, *args, **kwargs):
        raise NotImplementedError(
            "No implementation avialble to execute the workflow!!"
        )

    async def _execute(self, *args, **kwargs):
        # self.log("Starting workflow execution")
        # self.log(f"{self.workflow}")
        await self.run(*args, **kwargs)
        # self.log("Workflow execution finished")
        # self.log(f"{self.workflow}")

    def log(self, message: str, level=logging.INFO):
        msg = f"[{self.tracer_key}] {message}"
        logger.log(level, msg)
        self.messages.append(message)
        self._workflow.touch()
