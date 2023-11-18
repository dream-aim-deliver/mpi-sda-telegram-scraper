from enum import Enum
from typing import List, TypeVar
from pydantic import BaseModel, Field
from datetime import datetime

class BaseJobState(Enum):
    CREATED = "created"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"

class BaseJob(BaseModel):
    id: int
    tracer_id: str = Field(description="A unique identifier to trace jobs across the SDA runtime.")
    created_at: datetime = datetime.now()
    heartbeat: datetime = datetime.now()
    name: str
    state: Enum = BaseJobState.CREATED
    messages: List[str] = []
    output_lfn: List[str]
    input_lfns: List[str] | None = None

TBaseJob = TypeVar("TBaseJob", bound=BaseJob)

