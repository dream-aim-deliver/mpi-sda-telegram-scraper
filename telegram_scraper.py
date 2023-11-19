import os
import logging
import pandas as pd  # type: ignore
from telethon.sync import TelegramClient  # type: ignore
from app.sdk.minio_gateway import MinIORepository
from app.sdk.models import LFN, BaseJobState, DataSource, Protocol

from app.telegram_scraper_impl import TelegramScraperJob

logger = logging.getLogger(__name__)


async def scrape(
    job: TelegramScraperJob,
    channel_name: str,
    api_id: str,
    api_hash: str,
    minio_repository: MinIORepository,
    protocol: Protocol = Protocol.S3,
) -> None:
    try:
        async with TelegramClient("sda-telegram-scraper", api_id, api_hash) as client:
            outfile_lfn: LFN = LFN(
                protocol=protocol,
                tracer_id=job.tracer_id,
                job_id=job.id,
                source=DataSource.TELEGRAM,
                relative_path="data2_climate.csv",
            )
            logger.info(f"Starting Job {job}")
            outfile = minio_repository.lfn_to_pfn(outfile_lfn)
            logger.info(f"Output will be saved to: {outfile}")

            # Set the job state to running
            job.state = BaseJobState.RUNNING
            job.touch()

            data = []
            try:
                async for message in client.iter_messages(
                    f"https://t.me/{channel_name}"
                ):
                    logger.info(message.sender_id, ":", message.text, message.date)
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
                            pfn = generate_pfn(
                                protocol,
                                DataSource.TELEGRAM,
                                job.tracer_id,
                                job.id,
                                "photos",
                            )
                            # Download photo
                            file_location = await client.download_media(
                                message.media.photo,
                                file=os.path.join(".", pfn),
                            )
                            logger.info(f"Downloaded photo: {file_location}")
                            media_lfn: LFN = LFN(
                                protocol=protocol,
                                tracer_id=job.tracer_id,
                                job_id=job.id,
                                source=DataSource.TELEGRAM,
                                relative_path="photos",
                            )
                            job.output_lfns.append(media_lfn)
                            job.touch()
                        elif hasattr(message.media, "document"):
                            # Download video (or other documents)
                            pfn = generate_pfn(
                                protocol,
                                DataSource.TELEGRAM,
                                job.tracer_id,
                                job.id,
                                "videos",
                            )
                            file_location = await client.download_media(
                                message.media.document,
                                file=os.path.join(".", pfn),
                            )
                            logger.info(f"Downloaded video: {file_location}")
                            document_lfn: LFN = LFN(
                                protocol=protocol,
                                tracer_id=job.tracer_id,
                                job_id=job.id,
                                source=DataSource.TELEGRAM,
                                relative_path="videos",
                                pfn=file_location,
                            )
                            job.output_lfns.append(document_lfn)
                            job.touch()
            except Exception as error:
                job.state = BaseJobState.FAILED
                job.messages.append(f"Status: FAILED. Unable to scrape data. {error}")  # type: ignore
                job.touch()

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
                df.to_csv(outfile, encoding="utf-8")
            except:
                job.state = BaseJobState.FAILED
                job.messages.append("Status: FAILED. Unable to save data to CSV file. ")
                job.touch()
            job.output_lfns.append(outfile_lfn)
            job.state = BaseJobState.FINISHED
            job.touch()

    except Exception as e:
        job.state = BaseJobState.FAILED
        job.messages.append(f"Status: FAILED. Unable to scrape data. {e}")
