import logging
import os
import json
import httpx

from app.sdk.models import LFN

logger = logging.getLogger(__name__)


class KernelPlancksterGateway:
    def __init__(self, host: str, port: str, auth_token: str, scheme: str) -> None:
        self._host = host
        self._port = port
        self._auth_token = auth_token
        self._scheme = scheme

    @property
    def url(self) -> str:
        return f"{self._scheme}://{self._host}:{self._port}"

    def ping(self) -> bool:
        logger.info(f"Pinging Kernel Plankster Gateway at {self.url}")
        res = httpx.get(f"{self.url}/ping")
        logger.info(f"Ping response: {res.text}")
        return res.status_code == 200

    def register_new_data(self, pfns: list[str]) -> None:

        if isinstance(pfns, str):
            pfns = [pfns]

        if not self.ping():
            raise Exception("Failed to ping Kernel Plankster Gateway")

        logger.info(f"Registering new data with Kernel Plankster Gateway at {self.url}")
        knowledge_source_id = 1

        data = {
            "lfns": pfns,
        }

        endpoint = f"{self.url}/knowledge_source/{knowledge_source_id}/source_data"

        headers = {
            "Content-Type": "application/json",
            "x-auth-token": self._auth_token,
            }

        res = httpx.post(
            endpoint, json=pfns, headers=headers
        )

        logger.info(f"Register new data response: {res.text}")
        if res.status_code != 200:
            raise ValueError(
                f"Failed to register new data with Kernel Plankster Gateway: {res.text}"
            )
        logger.info(
            f"Successfully registered new data with Kernel Plankster Gateway {pfns}"
        )
