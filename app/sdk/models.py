from enum import Enum
import random
import re
import string
from typing import List, TypeVar
from pydantic import BaseModel, Field, field_validator, model_validator
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

    @field_validator("relative_path")
    def relative_path_must_be_alphanumberic_underscores_backslashes(cls, v):
        marker = "sdamarker"
        if marker not in v:
            v = re.sub(r"[^a-zA-Z0-9_\./-]", "", v)
            ext = v.split(".")[-1]
            name = v.split(".")[0]
            seed = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
            v = f"{name}-{seed}-{marker}.{ext}"
        return v

    def __str__(self):
        return f"{self.protocol.value}://{self.tracer_id}/{self.source.value}/{self.job_id}/{self.relative_path}"


class BaseJob(BaseModel):
    id: int
    tracer_id: str = Field(
        description="A unique identifier to trace jobs across the SDA runtime."
    )
    created_at: datetime = datetime.now()
    heartbeat: datetime = datetime.now()
    name: str
    args: dict = {}
    state: Enum = BaseJobState.CREATED
    messages: List[str] = []
    output_lfns: List[LFN] = []
    input_lfns: List[LFN] = []

    def touch(self) -> None:
        self.heartbeat = datetime.now()


TBaseJob = TypeVar("TBaseJob", bound=BaseJob)


class JobOutput(BaseModel):
    """
    This class is used to represent the output of a scraper job.

    Attributes:
    - job_state: BaseJobState
    - trace_id: str
    - lfns: List[LFN]
    """

    job_state: BaseJobState
    tracer_id: str
    lfns: List[LFN] | None

