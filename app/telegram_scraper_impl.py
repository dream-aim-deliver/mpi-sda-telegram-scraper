from typing import Any

from app.sdk.job_manager import BaseJobManager
from app.sdk.models import BaseJob, BaseJobState
from enum import Enum


class TelegramScraperStage(Enum):
    """
    Represents the stages of the Telegram Scraper job in addition to the
    BaseJobState stages.
    """

    UPLOADING = "uploading"


class TelegramScraperJob(BaseJob):
    """
    Represents a Telegram Scraper job.
    """

    state: BaseJobState | TelegramScraperStage = BaseJobState.CREATED


class TelegramScraperJobManager(BaseJobManager[TelegramScraperJob]):
    def __init__(self) -> None:
        super().__init__()

    def make(
        self, tracer_id: str, id: str, *args: Any, **kwargs: Any
    ) -> TelegramScraperJob:
        job = TelegramScraperJob(
            id=int(id),
            tracer_id=tracer_id,
            name=f"telegram-{id}",
        )
        return job
