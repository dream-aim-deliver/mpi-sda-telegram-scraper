from abc import ABC, abstractmethod
from typing import Dict, Generic, List

from app.sdk.models import BaseJob, TBaseJob


class BaseJobManager(ABC, Generic[TBaseJob]):
    def __init__(self) -> None:
        self._jobs: Dict[int, TBaseJob] = {} 
        self._nonce = 0
    
    @property
    def jobs(self) -> Dict[int, TBaseJob]:
        return self._jobs
    
    @property
    def nonce(self) -> int:
        return self._nonce
    
    @nonce.setter
    def nonce(self, value: int) -> None:
        self._nonce = value
        return None

    @abstractmethod
    def create_job(self) -> BaseJob:
        raise NotImplementedError
        self.nonce = self.nonce + 1
        job = BaseJob(
            id=self.nonce,
            name=f"telegram-{self.nonce}",
            output_lfn=[f"/telegram/{self.nonce}/data2_climate.csv"],
        )
        self.jobs[self.nonce] = job
        return job
    
    def get_job(self, job_id: int) -> TBaseJob:
        return self.jobs[job_id]
    
    def list_jobs(self) -> List[TBaseJob]:
        return list(self._jobs.values())