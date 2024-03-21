import logging
import os
import json
from typing import Tuple
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

    def generate_signed_url(self, lfn: LFN) -> str:
        if not self.ping():
            logger.error(f"Failed to ping Kernel Plankster Gateway at {self.url}")
            raise Exception("Failed to ping Kernel Plankster Gateway")

        logger.info(f"Generating signed url for {lfn.relative_path}")

        endpoint = f"{self.url}/get_client_data_for_upload"

        lfn_str = lfn.to_json()

        headers = {
            "Content-Type": "application/json",
            "x-auth-token": self._auth_token,
            }

        res = httpx.get(
            url=endpoint,
            params={"lfn": lfn},
            headers=headers,
        )

        logger.info(f"Generate signed url response: {res.text}")
        if res.status_code != 200:
            raise ValueError(f"Failed to generate signed url: {res.text}")

        res_json = res.json()

        res_lfn_json = res_json.get("lfn")
        signed_url = res_json.get("signed_url")

        if not res_lfn_json or not signed_url:
            raise ValueError(f"Failed to generate signed url. Dumping raw response:\n{res_json}")

        res_lfn_str = json.dumps(res_lfn_json)
        res_lfn = LFN.from_json(res_lfn_str)

        assert res_lfn == lfn

        return signed_url
        

    def register_new_source_data(self, lfn: LFN) -> dict[str, str]:
        """
        Registers new source data with Kernel Plankster Gateway.

        :param lfn: The LFN of the source data to register.
        :return: The registered source data. It has the "lfn" key, which contains the LFN of the registered source data, and should match the input LFN.
        """
        if not self.ping():
            logger.error(f"Failed to ping Kernel Plankster Gateway at {self.url}")
            raise Exception("Failed to ping Kernel Plankster Gateway")

        logger.info(f"Registering new data with Kernel Plankster Gateway at {self.url}")
        knowledge_source_id = 1  # NOTE: this should match the default knowledge source id in the database for Telegram

        lfn_str = lfn.to_json()

        data = {
            "lfn": lfn_str,
        }

        endpoint = f"{self.url}/knowledge_source/{knowledge_source_id}/source_data"

        headers = {
            "Content-Type": "application/json",
            "x-auth-token": self._auth_token,
            }

        res = httpx.post(
            endpoint,
            params=data,
            headers=headers,
        )

        logger.info(f"Register new data response: {res.text}")
        if res.status_code != 200:
            raise ValueError(
                f"Failed to register new data with Kernel Plankster Gateway: {res.text}"
            )

        source_data = res.json().get("source_data")

        if not source_data:
            raise ValueError(f"Failed to register new data. Source Data not returned. Dumping raw response:\n{res.json()}")

        res_lfn = source_data.get("lfn")

        if not res_lfn:
            raise ValueError(f"Failed to register new data. LFN not found in Source Data. Dumping raw response:\n{source_data}")

        res_lfn_str = json.dumps(res_lfn)
        res_lfn = LFN.from_json(res_lfn_str)

        assert res_lfn == lfn

        source_data["lfn"] = res_lfn

        return source_data
