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

    def log(self, message: str, level: int = logging.INFO) -> None:
        message = f"Kernel Plankster Gateway: {message}"
        logger.log(level, message)

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
        """
        Ping the Kernel Plankster Gateway to check if it is available

        Returns:
            bool: True if the Kernel Plankster Gateway is available, False otherwise

        """
        try:
            self.log(f"Pinging Kernel Plankster Gateway at {self.url}")
            res = httpx.get(f"{self.url}/ping")
            self.log(f"Ping response: {res.text}")
            return res.status_code == 200
        except Exception as e:
            self.log(
                f"Failed to ping Kernel Plankster Gateway: {e}", level=logging.ERROR
            )
            return False

    def register_new_data(
        self, knowledge_source: KnowledgeSourceEnum, pfns: list[str]
    ) -> None:
        """
        Register newly downloaded data with Kernel Planckster
        **NOTE** ONLY REGISTER DATA THAT HAS BEEN SUCCESSFULLY UPLOADED TO MINIO REPOSITORY
        """
        if isinstance(pfns, str):
            pfns = [pfns]
        if not self.ping():
            raise Exception("Failed to ping Kernel Plankster Gateway")
        self.log(f"Registering new data with Kernel Plankster Gateway at {self.url}")
        knowledge_source_id = self._get_kp_ks_id(knowledge_source)
        endpoint = f"{self.url}/knowledge_source/{knowledge_source_id}/source_data"

        try:
            res = httpx.post(
                endpoint, json=pfns, headers={"Content-Type": "application/json"}
            )
            self.log(f"Register new data response: {res.text}")
            if res.status_code != 200:
                raise ValueError(
                    f"Failed to register new data with Kernel Plankster Gateway: {res.text}"
                )
            self.log(
                f"Successfully registered new data with Kernel Plankster Gateway {pfns}"
            )
        except Exception as e:
            self.log(
                f"Failed to register new data with Kernel Plankster Gateway: {e}",
                level=logging.ERROR,
            )
            raise e
