import logging
import os
import shutil

import requests
from app.sdk.models import LFN, KnowledgeSourceEnum, ProtocolEnum

logger = logging.getLogger(__name__)

class FileRepository:
    def __init__(
            self,
            protocol: ProtocolEnum,
            data_dir: str = "data",  # can be used for config
    ) -> None:
        self._protocol = protocol
        self._data_dir = data_dir

    @property
    def protocol(self) -> ProtocolEnum:
        return self._protocol

    @property
    def data_dir(self) -> str:
        return self._data_dir
    
    def file_name_to_pfn(self, file_name: str) -> str:
        return f"{self.protocol}://{file_name}"

    def pfn_to_file_name(self, pfn: str) -> str:
        return pfn.split("://")[1]
    
    def lfn_to_file_name(self, lfn: LFN) -> str:
        return f"{self.data_dir}/{lfn.tracer_id}/{lfn.source.value}/{lfn.job_id}/{lfn.relative_path}"

    def save_file_locally(self, file_to_save: str, lfn: LFN, file_type: str) -> str:
        """
        Save a file to a local directory.

        :param file_to_save: The path to the file to save.
        :param lfn: The LFN to save the file as.
        :param file_type: The type of file being saved.
        """
        
        file_name = self.lfn_to_file_name(lfn)
        logger.info(f"Saving {file_type} '{lfn}' to '{file_name}'.")

        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        shutil.copy(file_to_save, file_name)

        logger.info(f"Saved {file_type} '{lfn}' to '{file_name}'.")

        pfn = self.file_name_to_pfn(file_name)

        return pfn

        
    def public_upload(self, signed_url: str, file_path: str) -> None:
        """
        Upload a file to a signed url.

        :param signed_url: The signed url to upload to.
        :param file_path: The path to the file to upload.
        """

        with open(file_path, "rb") as f:
            upload_res = requests.put(signed_url, data=f)

        if upload_res.status_code != 200:
            raise ValueError(f"Failed to upload file to signed url: {upload_res.text}")

