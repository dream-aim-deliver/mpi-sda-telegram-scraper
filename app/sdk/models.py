from enum import Enum
from typing import Generic, List, TypeVar
from pydantic import BaseModel
from datetime import datetime
class BaseJobState(Enum):
    CREATED = "created"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"

TBaseJobState = TypeVar("TBaseJobState", bound=BaseJobState)

class BaseJob(BaseModel, Generic[TBaseJobState]):
    id: int
    tracer_id: str
    created_at: datetime = datetime.now()
    heartbeat: datetime = datetime.now()
    name: str
    state: BaseJobState = BaseJobState.CREATED
    messages: List[str] = []
    output_lfn: List[str]
    input_lfns: List[str] | None = None
    status: str = "created"

TBaseJob = TypeVar("TBaseJob", bound=BaseJob)

