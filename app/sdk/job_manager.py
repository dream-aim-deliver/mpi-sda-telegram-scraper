from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List
import os
from pydantic import BaseModel

from app.sdk.models import BaseJob, TBaseJob


class BaseJobManager(ABC, Generic[TBaseJob]):
    def __init__(self) -> None:
        self._jobs: Dict[int, TBaseJob] = {}
        self._nonce = 0

    @property
    def name(self) -> str:
        return os.getenv("JOB_MANAGER_NAME", "default")

    @property
    def jobs(self) -> Dict[int, TBaseJob]:
        return self._jobs

    @property
    def nonce(self) -> int:
        self._nonce = self._nonce + 1
        return self._nonce

    @abstractmethod
    def make(self, *args: Any, **kwargs: Any) -> TBaseJob:
        """
        Return a new Job object. This function is called when a new job is
        requested.
        The parameters are decided by the subclass.
        """
        raise NotImplementedError("make method must be implemented in a subclass.")

    def create_job(
        self, tracer_id: str, job_args: Dict[str, Any], *args: Any, **kwargs: Any
    ) -> BaseJob:
        id = self.nonce
        job = BaseJob(
            id=id,
            name=f"{self.name}-{id}",
            tracer_id=tracer_id,
            args=job_args,
        )

        self.jobs[job.id] = job  # type: ignore
        return job

    def get_job(self, job_id: int) -> TBaseJob:
        return self.jobs[job_id]

    def list_jobs(self) -> List[TBaseJob]:
        return list(self._jobs.values())
