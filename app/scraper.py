from logging import Logger
import logging
import os
import tempfile
from typing import List
from telethon import TelegramClient
from app.sdk.models import KernelPlancksterSourceData, BaseJobState, JobOutput
from app.sdk.scraped_data_repository import ScrapedDataRepository



async def scrape(
    job_id: int,
    channel_name: str,
    tracer_id: str,
    scraped_data_repository: ScrapedDataRepository,
    telegram_client: TelegramClient,
    log_level: Logger,
) -> JobOutput:


    try:
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=log_level)

        job_state = BaseJobState.CREATED
        current_data: KernelPlancksterSourceData | None = None
        last_successful_data: KernelPlancksterSourceData | None = None

        protocol = scraped_data_repository.protocol

        output_data_list: List[KernelPlancksterSourceData] = []
        async with telegram_client as client:
            assert isinstance(client, TelegramClient)  # for typing

            # Set the job state to running
            logger.info(f"{job_id}: Starting Job")
            job_state = BaseJobState.RUNNING
            #job.touch()

            data = []

            try:
                async for message in client.iter_messages(
                    f"https://t.me/{channel_name}"
                ):
                    ############################################################
                    # IF YOU CAN ALREADY VALIDATE YOUR DATA HERE
                    # YOU MIGHT NOT NEED A LLM TO FIX ISSUES WITH THE DATA
                    ############################################################
                    logger.info(f"message: {message}")
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

                        if hasattr(message.media, "photo") and message.media.photo is not None:

                            # Download photo
                            with tempfile.NamedTemporaryFile() as tmp:
                                logger.info(
                                    f"{job_id}: Downloading photo to {tmp.name}"
                                )
                                file_location = await client.download_media(
                                    message.media.photo,
                                    file=tmp.name
                                )

                                logger.info(
                                    f"{job_id}: Downloaded photo: {file_location}"
                                )

                                file_name = f"{os.path.basename(tmp.name)}"
                                relative_path = f"telegram/{tracer_id}/{job_id}/photos/{channel_name}-{file_name}.photo"

                                data_name = os.path.splitext(file_name)[0]

                                media_data = KernelPlancksterSourceData(
                                    name=data_name,
                                    protocol=protocol,
                                    relative_path=relative_path,
                                )

                                current_data = media_data
                                

                                scraped_data_repository.register_scraped_photo(
                                    job_id=job_id,
                                    source_data=media_data,
                                    local_file_name=tmp.name,
                                )

                                output_data_list.append(media_data)
                                #job.touch()
                            
                                last_successful_data = media_data

                        elif hasattr(message.media, "document") and message.media.document is not None:

                            # Download video (or other documents)
                            with tempfile.NamedTemporaryFile() as tmp:

                                file_location = await client.download_media(
                                    message.media.document,
                                    file=tmp.name,
                                )
                                logger.info(
                                    f"{job_id}: Downloaded video: {file_location}"
                                )

                                file_name = f"{os.path.basename(tmp.name)}"
                                relative_path = f"telegram/{tracer_id}/{job_id}/videos/{channel_name}-{file_name}.video"
                                data_name = os.path.splitext(file_name)[0]

                                document_data = KernelPlancksterSourceData(
                                    name=data_name,
                                    protocol=protocol,
                                    relative_path=relative_path,
                                )

                                current_data = document_data

                                scraped_data_repository.register_scraped_video_or_document(
                                    job_id=job_id,
                                    source_data=document_data,
                                    local_file_name=tmp.name,
                                )

                                output_data_list.append(document_data)
                                #job.touch()
                                last_successful_data = document_data


            except Exception as error:
                job_state = BaseJobState.FAILED
                logger.error(
                    f"{job_id}: Unable to scrape data. Error:\n{error}\nJob with tracer_id {tracer_id} failed.\nLast successful data: {last_successful_data}\nCurrent data: \"{current_data}\", job_state: \"{job_state}\""
                )
                #job.messages.append(f"Status: FAILED. Unable to scrape data. {error}")  # type: ignore
                #job.touch()

                # continue to scrape data if possible


            job_state = BaseJobState.FINISHED
            #job.touch()
            logger.info(f"{job_id}: Job finished")

            return JobOutput(
                job_state=job_state,
                tracer_id=tracer_id,
                source_data_list=output_data_list,
            )


    except Exception as error:
        logger.error(f"{job_id}: Unable to scrape data. Job with tracer_id {tracer_id} failed. Error:\n{error}")
        job_state = BaseJobState.FAILED
        #job.messages.append(f"Status: FAILED. Unable to scrape data. {e}")