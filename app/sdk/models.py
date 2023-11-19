from enum import Enum
from typing import List, TypeVar
from pydantic import BaseModel, Field, model_validator
from datetime import datetime


class BaseJobState(Enum):
    CREATED = "created"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


class DataSource(Enum):
    TWITTER = "twitter"
    TELEGRAM = "telegram"
    SENTINEL = "sentinel"
    AUGMENTED_DATA = "augmented_data"


class Protocol(Enum):
    S3 = "s3"
    ES = "es"
    LOCAL = "local"


class LFN(BaseModel):
    protocol: Protocol
    tracer_id: str
    job_id: int
    source: DataSource
    relative_path: str


class BaseJob(BaseModel):
    id: int
    tracer_id: str = Field(
        description="A unique identifier to trace jobs across the SDA runtime."
    )
    created_at: datetime = datetime.now()
    heartbeat: datetime = datetime.now()
    name: str
    state: Enum = BaseJobState.CREATED
    messages: List[str] = []
    output_lfns: List[LFN] = []
    input_lfns: List[LFN] = []

    def touch(self) -> None:
        self.heartbeat = datetime.now()


TBaseJob = TypeVar("TBaseJob", bound=BaseJob)
