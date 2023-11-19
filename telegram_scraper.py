import os
import logging
import shutil
import pandas as pd  # type: ignore
from telethon.sync import TelegramClient
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway  # type: ignore
from app.sdk.minio_gateway import MinIORepository
from app.sdk.models import LFN, BaseJob, BaseJobState, DataSource, Protocol

import tempfile


logger = logging.getLogger(__name__)


async def scrape(
    job: BaseJob,
    kernel_planckster: KernelPlancksterGateway,
    minio_repository: MinIORepository,
    protocol: Protocol = Protocol.S3,
) -> None:
    try:
        api_id = os.getenv("API_ID")
        api_hash = os.getenv("API_HASH")
        if "channel_name" not in job.args:
            # YOU CAN RAISE AN ERROR HERE IF YOU WANT
            # raise ValueError("channel_name must be set.")
            channel_name = "GCC_report"
        else:
            channel_name = job.args["channel_name"]

        if api_id is None or api_hash is None:
            raise ValueError("API_ID and API_HASH for telegram must be set.")
        async with TelegramClient("sda-telegram-scraper", api_id, api_hash) as client:
            logger.info(f"{job.id}: Starting Job {job}")

            # Set the job state to running
            job.state = BaseJobState.RUNNING
            job.touch()

            data = []
            try:
                async for message in client.iter_messages(
                    f"https://t.me/{channel_name}"
                ):
                    ##################################################################
                    # IF YOU CAN ALREADY VALIDATE YOUR DATA HERE
                    # YOU MIGHT NOT NEED A LLM TO FIX ISSUES WITH THE DATA
                    ##################################################################
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
                        if hasattr(message.media, "photo"):
                            # Download photo
                            with tempfile.NamedTemporaryFile() as tmp:
                                logger.info(
                                    f"{job.id}: Downloading photo to {tmp.name}"
                                )
                                file_location = await client.download_media(
                                    message.media.photo,
                                    file=tmp.name,
                                )
                                logger.info(
                                    f"{job.id}: Downloaded photo: {file_location}"
                                )
                                media_lfn: LFN = LFN(
                                    protocol=protocol,
                                    tracer_id=job.tracer_id,
                                    job_id=job.id,
                                    source=DataSource.TELEGRAM,
                                    relative_path=f"photos",
                                )
                                if protocol == Protocol.S3:
                                    pfn = minio_repository.lfn_to_pfn(media_lfn)
                                    logger.debug(
                                        f"{job.id}:Uploading photo {media_lfn} to {pfn}"
                                    )
                                    minio_repository.upload_file(media_lfn, tmp.name)
                                    logger.info(
                                        f"{job.id}: Uploaded photo {media_lfn} to {pfn}"
                                    )
                                elif protocol == Protocol.LOCAL:
                                    pfn = f"data/{media_lfn.tracer_id}/{media_lfn.source.value}/{media_lfn.job_id}/{media_lfn.relative_path}"
                                    logger.debug(
                                        f" {job.id}:Saving photo {media_lfn} locally to {pfn}"
                                    )
                                    os.makedirs(os.path.dirname(pfn), exist_ok=True)
                                    shutil.copy(tmp.name, pfn)
                                    logger.info(
                                        f"{job.id}: Saved photo {media_lfn} to {pfn}"
                                    )

                                job.output_lfns.append(media_lfn)
                                job.touch()
                        elif hasattr(message.media, "document"):
                            # Download video (or other documents)
                            with tempfile.NamedTemporaryFile() as tmp:
                                file_location = await client.download_media(
                                    message.media.document,
                                    file=tmp.name,
                                )
                                logger.info(
                                    f"{job.id}: Downloaded video: {file_location}"
                                )
                                document_lfn: LFN = LFN(
                                    protocol=protocol,
                                    tracer_id=job.tracer_id,
                                    job_id=job.id,
                                    source=DataSource.TELEGRAM,
                                    relative_path="videos",
                                )
                                if protocol == Protocol.S3:
                                    pfn = minio_repository.lfn_to_pfn(document_lfn)
                                    logger.debug(
                                        f" {job.id}: Uploading video {document_lfn} to {pfn}"
                                    )
                                    minio_repository.upload_file(
                                        document_lfn,
                                        file_location,
                                    )
                                    logger.info(
                                        f"{job.id}: Uploaded video {document_lfn} to {pfn}"
                                    )
                                elif protocol == Protocol.LOCAL:
                                    pfn = f"data/{document_lfn.tracer_id}/{document_lfn.source.value}/{document_lfn.job_id}/{document_lfn.relative_path}"
                                    logger.debug(
                                        f"{job.id}: Saving video {document_lfn} locally to {pfn}"
                                    )
                                    os.makedirs(os.path.dirname(pfn), exist_ok=True)
                                    shutil.copy(tmp.name, pfn)
                                    logger.info(
                                        f"{job.id}: Saved video {document_lfn} to {pfn}"
                                    )
                                job.output_lfns.append(document_lfn)
                                job.touch()
                                kernel_planckster.register_new_data(
                                    pfns=[
                                        pfn,
                                    ],
                                )
            except Exception as error:
                logger.error(
                    f"{job.id}: Unable to scrape data. {error}. Job {job} failed."
                )
                job.state = BaseJobState.FAILED
                job.messages.append(f"Status: FAILED. Unable to scrape data. {error}")  # type: ignore
                job.touch()
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
                    protocol=protocol,
                    tracer_id=job.tracer_id,
                    job_id=job.id,
                    source=DataSource.TELEGRAM,
                    relative_path="data2_climate.csv",
                )
                if protocol == Protocol.LOCAL:
                    pfn = f"data/{outfile_lfn.tracer_id}/{outfile_lfn.source.value}/{outfile_lfn.job_id}/{outfile_lfn.relative_path}"
                    df.to_csv(pfn, encoding="utf-8")
                    logger.info(f"{job.id}: Saved data to {pfn}")
                elif protocol == Protocol.S3:
                    with tempfile.NamedTemporaryFile() as tmp:
                        df.to_csv(tmp.name, encoding="utf-8")
                        minio_repository._upload_file(
                            minio_repository.bucket,
                            minio_repository.lfn_to_pfn(outfile_lfn),
                            tmp.name,
                        )
                        logger.info(f"{job.id}: Uploaded data to {pfn}")
                else:
                    raise ValueError(f"Protocol {protocol} is not supported.")
                job.output_lfns.append(outfile_lfn)
                job.state = BaseJobState.FINISHED
                job.touch()
            except:
                logger.error(
                    f"{job.id}: Unable to save data to CSV file. Job {job} failed."
                )
                job.state = BaseJobState.FAILED
                job.messages.append("Status: FAILED. Unable to save data to CSV file. ")
                job.touch()

    except Exception as e:
        logger.error(f"{job.id}: Unable to scrape data. {e}. Job {job} failed.")
        job.state = BaseJobState.FAILED
        job.messages.append(f"Status: FAILED. Unable to scrape data. {e}")
