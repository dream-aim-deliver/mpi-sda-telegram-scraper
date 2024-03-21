from enum import Enum
import os
import re
from typing import List, TypeVar
import uuid
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime


class BaseJobState(Enum):
    CREATED = "created"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


class KnowledgeSourceEnum(Enum):
    TWITTER = "twitter"
    TELEGRAM = "telegram"
    SENTINEL = "sentinel"
    AUGMENTED_DATA = "augmented_data"


class ProtocolEnum(Enum):
    """
    The storage protocol to use for a file.

    Attributes:
    - S3: S3
    - LOCAL: Local  @deprecated
    """
    S3 = "s3"
    LOCAL = "local"


class LFN(BaseModel):
    """
    Synchronize this with Kernel Planckster's LFN model, so that this client generates valid requests.

    Attributes:
    - protocol: ProtocolEnum
    - tracer_id: str
    - job_id: int
    - source: KnowledgeSourceEnum
    - relative_path: str
    """
    protocol: ProtocolEnum
    tracer_id: str
    job_id: int
    source: KnowledgeSourceEnum
    relative_path: str

    @field_validator("relative_path")
    def relative_path_must_be_alphanumberic_underscores_backslashes(cls, v: str) -> str:
        marker = "sdamarker"
        if marker not in v:
            v = os.path.basename(v)  # Take just the basename, saner for the object stores
            v = re.sub(r"[^a-zA-Z0-9_\./-]", "", v)
            ext = v.split(".")[-1]
            name = v.split(".")[0]  # this completely removes dots
            seed = f"{uuid.uuid4()}".replace("-", "")
            v = f"{name}-{seed}-{marker}.{ext}"
        return v

    def to_json(cls) -> str:
        """
        Dumps the model to a json formatted string. Wrapper around pydantic's model_dump_json method: in case they decide to deprecate it, we only refactor here.
        """
        return cls.model_dump_json()

    def __str__(self) -> str:
        return self.to_json()

    @classmethod
    def from_json(cls, json_str: str) -> "LFN":
        """
        Loads the model from a json formatted string. Wrapper around pydantic's model_validate_json method: in case they decide to deprecate it, we only refactor here.
        """
        return cls.model_validate_json(json_data=json_str)


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

