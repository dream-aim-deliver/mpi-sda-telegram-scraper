import logging
import os

import httpx

from app.sdk.models import LFN

logger = logging.getLogger(__name__)


class KernelPlancksterGateway:
    def __init__(self, host: str, port: str) -> None:
        self._host = host
        self._port = port

    @property
    def _url(self) -> str:
        return f"{self._host}:{self._port}"

    def ping(self) -> bool:
        logger.info(f"Pinging Kernel Plankster Gateway at {self._url}")
        res = httpx.get(f"{self._url}/ping")
        logger.info(f"Ping response: {res.text}")
        return res.status_code == 200
