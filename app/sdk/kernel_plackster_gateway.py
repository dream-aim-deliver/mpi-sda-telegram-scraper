from enum import Enum
import logging
import os
import json
import httpx

from app.sdk.models import LFN

logger = logging.getLogger(__name__)


class KnowledgeSourceEnum(Enum):
    """
    Enum for the different knowledge sources that can be used to create a research context.

    TELEGRAM: the knowledge source is a Telegram channel
    TWITTER: the knowledge source is a Twitter account
    AUGMENTED: the knowledge source is a collection of user uploads
    SENTINEL: the knowledge source is a collection of user uploads, and the user wants to be notified when new uploads are available
    """

    TELEGRAM = "telegram"
    TWITTER = "twitter"
    AUGMENTED = "augmented"
    SENTINEL = "sentinel"


class KernelPlancksterGateway:
    def __init__(self, host: str, port: str) -> None:
        self._host = host
        self._port = port

    @property
    def url(self) -> str:
        return f"{self._host}:{self._port}"

    def _get_kp_ks_id(self, data_source: KnowledgeSourceEnum) -> int:
        if data_source == KnowledgeSourceEnum.TELEGRAM:
            return 1
        elif data_source == KnowledgeSourceEnum.TWITTER:
            return 2
        elif data_source == KnowledgeSourceEnum.AUGMENTED:
            return 3
        elif data_source == KnowledgeSourceEnum.SENTINEL:
            return 4
        else:
            raise ValueError(f"Unknown data source {data_source}")

    def ping(self) -> bool:
        logger.info(f"Pinging Kernel Plankster Gateway at {self.url}")
        res = httpx.get(f"{self.url}/ping")
        logger.info(f"Ping response: {res.text}")
        return res.status_code == 200

    def register_new_data(
        self, knowledge_source: KnowledgeSourceEnum, pfns: list[str]
    ) -> None:
        if isinstance(pfns, str):
            pfns = [pfns]
        if not self.ping():
            raise Exception("Failed to ping Kernel Plankster Gateway")
        logger.info(f"Registering new data with Kernel Plankster Gateway at {self.url}")
        knowledge_source_id = self._get_kp_ks_id(knowledge_source)
        data = {
            "lfns": pfns,
        }
        endpoint = f"{self.url}/knowledge_source/{knowledge_source_id}/source_data"
        res = httpx.post(
            endpoint, json=pfns, headers={"Content-Type": "application/json"}
        )
        logger.info(f"Register new data response: {res.text}")
        if res.status_code != 200:
            raise ValueError(
                f"Failed to register new data with Kernel Plankster Gateway: {res.text}"
            )
        logger.info(
            f"Successfully registered new data with Kernel Plankster Gateway {pfns}"
        )
