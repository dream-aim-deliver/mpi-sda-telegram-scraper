from enum import Enum
import random
import re
import string
from typing import Any, Dict, List, TypeVar
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime


class Status(Enum):
    CREATED = "created"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


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


class BaseWorkflow(BaseModel):
    id: int
    name: str
    created_at: datetime = datetime.now()
    heartbeat: datetime = datetime.now()
    tracer_key: str
    job_args: Dict[str, Any] = {}
    messages: List[str] = []
    output_lfns: List[LFN] = []
    input_lfns: List[LFN] = []

    def touch(self) -> None:
        self.heartbeat = datetime.now()


class BaseJob(BaseModel):
    id: int
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
