import asyncio
import logging
import os
import tempfile
from time import sleep
from typing import Any, Dict

from fastapi import FastAPI
import httpx
import pandas as pd
from telethon import TelegramClient
from app.sdk.kernel_plackster_gateway import (
    KernelPlancksterGateway,
    KnowledgeSourceEnum,
)
from app.sdk.minio_repository import MinIORepository
from app.sdk.models import LFN, STATE, Protocol, DataSource
from app.sdk.workflow_manager import BaseWorkflow, BaseWorkflowManager
from app.sdk.workflow_executor import BaseWorkflowExecutor
from telegram_scraper import scrape


class DataPreparationWorkflowManager(BaseWorkflowManager):
    def __init__(self, app: FastAPI):
        super().__init__(
            "sda_data_processing",
            app=app,
            kernel_plankster=app.kernel_plankster,  # type: ignore
            minio=app.minio_repository,  # type: ignore
        )

    def create_workflow_executor(
        self, workflow: BaseWorkflow, *args, **kwargs
    ) -> "DataPreparationWorkflowExecutor":
        if "telegram_channel" not in workflow.job_args:
            raise ValueError(
                "Invalid workflow! Missing required argument: telegram_channel"
            )
        # For Alpha, we are hardcoding the telegram channel to be used
        telegram_channel = workflow.job_args.get("telegram_channel", "GCC_report")
        executor = DataPreparationWorkflowExecutor(
            kernel_plankster=self.kernel_plankster,
            workflow=workflow,
            minio=self.minio,
            telegram_channel=telegram_channel,
        )
        return executor

    def route_create_workflow(self):
        @self.router.post(
            "/",
            name="Create a new Data Preparation Workflow",
        )
        def create_workflow(
            tracer_key: str,  # TIP: This is ALWAYS required to trace your workflow files across the system
            telegram_channel: str = "GCC_report",  # TIP: You can request explicit values for your workflow
            workflow_args: Dict[str, Any] = {
                "version": "1.0.0-alpha"
            },  # TIP: Here you can pass additional values that will be available in the workflow context
        ) -> BaseWorkflow:
            return self._create_workflow(
                tracer_key=tracer_key,
                telegram_channel=telegram_channel,
                **workflow_args,
            )


class DataPreparationWorkflowExecutor(BaseWorkflowExecutor):
    def __init__(
        self,
        workflow: BaseWorkflow,
        telegram_channel: str,
        kernel_plankster: KernelPlancksterGateway,
        minio: MinIORepository,
    ):
        super().__init__(
            workflow=workflow, kernel_plankster=kernel_plankster, minio=minio
        )
        self.telegram_channel = telegram_channel

    async def run(self, *args, **kwargs):
        """
        This is where you will implement your workflow logic.
        """
        self.log(f"{self.workflow}")
        await asyncio.sleep(5)
        self.kernel_plankster_gateway.ping()
        await self.scrape_telegram()

        self.log(level=logging.INFO, message=f"Args: {args}")
        self.log(level=logging.INFO, message=f"Kwargs: {kwargs}")

    async def scrape_telegram(self):
        try:
            api_id = os.getenv("API_ID")
            api_hash = os.getenv("API_HASH")
            channel_name = self.telegram_channel
            if api_id is None or api_hash is None:
                raise ValueError("API_ID and API_HASH for telegram must be set.")
            async with TelegramClient(
                "sda-telegram-scraper", api_id, api_hash
            ) as client:
                # Set the job state to running
                self.workflow.state = STATE.RUNNING
                self.workflow.touch()

                data = []
                try:
                    async for message in client.iter_messages(
                        f"https://t.me/{channel_name}"
                    ):
                        ##################################################################
                        # IF YOU CAN ALREADY VALIDATE YOUR DATA HERE
                        # YOU MIGHT NOT NEED A LLM TO FIX ISSUES WITH THE DATA
                        ##################################################################
                        self.log(f"message: {message}")
                        data.append(
                            [
                                message.sender_id,
                                message.text,
                                message.date,
                                message.id,
                                message.post_author,
                                message.views,
                                message.peer_id.channel_id,
                            ]
                        )

                        # Check if the message has media (photo or video)
                        if message.media:
                            if hasattr(message.media, "photo"):
                                # Download photo
                                with tempfile.NamedTemporaryFile() as tmp:
                                    self.log(f"Downloading photo to {tmp.name}")
                                    file_location = await client.download_media(
                                        message.media.photo,
                                        file=tmp.name,
                                    )
                                    self.log(f"Downloaded photo: {file_location}")
                                    media_lfn: LFN = LFN(
                                        protocol=Protocol.S3,
                                        tracer_id=self.workflow.tracer_key,
                                        workflow_id=self.workflow.id,
                                        source=DataSource.TELEGRAM,
                                        relative_path=f"photos/{file_location}",
                                    )
                                    pfn = self.minio_repository.lfn_to_pfn(media_lfn)
                                    self.log(f"uploading photo {media_lfn} to {pfn}")
                                    self.minio_repository.upload_file(
                                        media_lfn, tmp.name
                                    )
                                    self.log.info(
                                        f"Uploaded photo {media_lfn} to {pfn}"
                                    )
                                    self.workflow.output_lfns.append(media_lfn)
                                    self.workflow.output_lfns.append(document_lfn)
                                    self.kernel_plankster_gateway.register_new_data(
                                        knowledge_source=KnowledgeSourceEnum.TELEGRAM,
                                        pfns=[
                                            pfn,
                                        ],
                                    )
                            elif hasattr(message.media, "document"):
                                # Download video (or other documents)
                                with tempfile.NamedTemporaryFile() as tmp:
                                    file_location = await client.download_media(
                                        message.media.document,
                                        file=tmp.name,
                                    )
                                    self.logger.info(
                                        f"Downloaded video: {file_location}"
                                    )
                                    document_lfn: LFN = LFN(
                                        protocol=Protocol.S3,
                                        tracer_id=self.workflow.tracer_key,
                                        workflow_id=self.workflow.id,
                                        source=DataSource.TELEGRAM,
                                        relative_path=f"videos/{file_location}",
                                    )

                                    pfn = self.minio_repository.lfn_to_pfn(document_lfn)
                                    self.logger.debug(
                                        f" Uploading video {document_lfn} to {pfn}"
                                    )
                                    self.minio_repository.upload_file(
                                        document_lfn,
                                        file_location,
                                    )
                                    self.logger.info(
                                        f"Uploaded video {document_lfn} to {pfn}"
                                    )
                                    self.workflow.output_lfns.append(document_lfn)
                                    self.kernel_plankster_gateway.register_new_data(
                                        knowledge_source=KnowledgeSourceEnum.TELEGRAM,
                                        pfns=[
                                            pfn,
                                        ],
                                    )
                except Exception as error:
                    self.logger.error(f"Unable to scrape data. {error}")
                    self.workflow.state = STATE.FAILED
                    self.workflow.messages.append(f"Status: FAILED. Unable to scrape data. {error}")  # type: ignore
                    self.workflow.touch()
                    # TODO: continue to scrape data if possible

                # Save the data to a CSV file
                df = pd.DataFrame(
                    data,
                    columns=[
                        "message.sender_id",
                        "message.text",
                        "message.date",
                        "message.id",
                        "message.post_author",
                        "message.views",
                        "message.peer_id.channel_id",
                    ],
                )
                try:
                    outfile_lfn: LFN = LFN(
                        protocol=Protocol.S3,
                        tracer_id=self.workflow.tracer_key,
                        workflow_id=self.workflow.id,
                        source=DataSource.TELEGRAM,
                        relative_path="data2_climate.csv",
                    )

                    with tempfile.NamedTemporaryFile() as tmp:
                        df.to_csv(tmp.name, encoding="utf-8")
                        self.minio_repository.upload_file(outfile_lfn, tmp.name)
                        self.log(f"Uploaded data to {pfn}")

                    self.workflow.output_lfns.append(outfile_lfn)
                    self.workflow.state = STATE.FINISHED
                    self.workflow.touch()
                except:
                    self.log(f"Unable to save data to CSV file. FAILED!")
                    self.workflow.state = STATE.FAILED
                    self.workflow.messages.append(
                        "Status: FAILED. Unable to save data to CSV file. "
                    )

        except Exception as e:
            self.log(f"Unable to scrape data. {e}.", level=logging.ERROR)
            self.workflow.state = STATE.FAILED
            self.workflow.messages.append(f"Status: FAILED. Unable to scrape data. {e}")
