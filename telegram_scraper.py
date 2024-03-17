import os
import logging
import shutil
from typing import List, Tuple
import pandas as pd  # type: ignore
from telethon.sync import TelegramClient
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway  # type: ignore
from app.sdk.minio_gateway import MinIORepository
from app.sdk.models import LFN, BaseJobState, DataSource, Protocol, JobOutput
from dotenv import load_dotenv

import tempfile

job_state = BaseJobState.CREATED
current_lfn: LFN | None = None
last_successful_lfn: LFN | None = None

logger = logging.getLogger(__name__)


def _setup_kernel_planckster(
    job_id: int,
) -> KernelPlancksterGateway:

    try:

        logger.info(f"{job_id}: Setting up Kernel Planckster Gateway.")
        # Check environment variables for the Kernel Planckster Gateway
        kernel_planckster_host = os.getenv("KERNEL_PLANCKSTER_HOST")
        kernel_planckster_port = os.getenv("KERNEL_PLANCKSTER_PORT")
        kernel_planckster_auth_token = os.getenv("KERNEL_PLANCKSTER_AUTH_TOKEN")
        kernel_planckster_scheme = os.getenv("KERNEL_PLANCKSTER_SCHEME")

        if not all([kernel_planckster_host, kernel_planckster_port, kernel_planckster_auth_token, kernel_planckster_scheme]):
            logger.error(f"{job_id}: KERNEL_PLANCKSTER_HOST, KERNEL_PLANCKSTER_PORT, KERNEL_PLANCKSTER_AUTH_TOKEN and KERNEL_PLANCKSTER_SCHEME must be set.")
            raise ValueError("KERNEL_PLANCKSTER_HOST, KERNEL_PLANCKSTER_PORT, KERNEL_PLANCKSTER_AUTH_TOKEN and KERNEL_PLANCKSTER_SCHEME must be set.")

        # Setup the Kernel Planckster Gateway
        kernel_planckster = KernelPlancksterGateway(
            host=kernel_planckster_host,
            port=kernel_planckster_port,
            auth_token=kernel_planckster_auth_token,
            scheme=kernel_planckster_scheme,
        )
        kernel_planckster.ping()
        logger.info(f"{job_id}: Kernel Planckster Gateway setup successfully.")

        return kernel_planckster

    except Exception as error:
        logger.error(f"{job_id}: Unable to setup the Kernel Planckster Gateway. Error:\n{error}")
        raise error


def _setup_minio_repository(
    job_id: int,
) -> MinIORepository:
        
    try:
        logger.info(f"{job_id}: Setting up MinIO Repository.")
        
        # Check environment variables for the MinIO Repository
        minio_host = os.getenv("MINIO_HOST")
        minio_port = os.getenv("MINIO_PORT")
        minio_access_key = os.getenv("MINIO_ACCESS_KEY")
        minio_secret_key = os.getenv("MINIO_SECRET_KEY")
        minio_bucket = os.getenv("MINIO_BUCKET")

        if not all([minio_host, minio_port, minio_access_key, minio_secret_key, minio_bucket]):
            logger.error(f"{job_id}: MINIO_HOST, MINIO_PORT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY and MINIO_BUCKET must all be set.")
            raise ValueError("MINIO_HOST, MINIO_PORT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY and MINIO_BUCKET must be set.")

        # Setup the MinIO Repository and create the bucket if it does not exist
        minio_repository = MinIORepository(
            host=minio_host,
            port=minio_port,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            bucket=minio_bucket,
        )

        logger.info(f"{job_id}: Creating bucket {minio_repository.bucket} if it does not exist.")
        minio_repository.create_bucket_if_not_exists(minio_repository.bucket)

        logger.info(f"{job_id}: MinIO Repository setup successfully.")

        return minio_repository
    
    except Exception as error:
        logger.error(f"{job_id}: Unable to setup the MinIO Repository. Error:\n{error}")
        raise error


def _setup_telegram_client(
    job_id: int,
) -> TelegramClient:
    try:

        logger.info(f"{job_id}: Setting up Telegram client.")

        # Check environment variables for the Telegram client
        telegram_api_id = os.getenv("TELEGRAM_API_ID")
        telegram_api_hash = os.getenv("TELEGRAM_API_HASH")
        telegram_phone = os.getenv("TELEGRAM_PHONE")
        telegram_password = os.getenv("TELEGRAM_PASSWORD")
        telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")

        if not all([telegram_api_id, telegram_api_hash]):
            logger.error(f"{job_id}: TELEGRAM_API_ID and TELEGRAM_API_HASH must be set.")
            raise ValueError("TELEGRAM_API_ID and TELEGRAM_API_HASH must be set.")    

        if not (telegram_phone and telegram_password) and not telegram_bot_token:
            logger.error(f"{job_id}: Either TELEGRAM_PHONE and TELEGRAM_PASSWORD, or TELEGRAM_BOT_TOKEN must be set.")
            raise ValueError("Either TELEGRAM_PHONE and TELEGRAM_PASSWORD, or TELEGRAM_BOT_TOKEN must be set.")



        client = TelegramClient("sda-telegram-scraper", telegram_api_id, telegram_api_hash)

        if telegram_phone and telegram_password:
            logger.info(f"{job_id}: Starting Telegram client with phone number")
            client.start(
                phone=telegram_phone,
                password=telegram_password,
            )

        elif telegram_bot_token:
            logger.info(f"{job_id}: Starting Telegram client with bot token")
            client.start(
                bot_token=telegram_bot_token,
            )

        else:
            raise ValueError("Either TELEGRAM_PHONE and TELEGRAM_PASSWORD, or TELEGRAM_BOT_TOKEN must be set.")

        logger.info(f"{job_id}: Telegram client started successfully")

        return client


    except Exception as error:
        logger.error(f"{job_id}: Unable to setup the Telegram client. Error:\n{error}")
        raise error


def _setup(
    job_id: int,
) -> Tuple[KernelPlancksterGateway, Protocol, MinIORepository | None, TelegramClient]:

    try:

        load_dotenv(
            dotenv_path=".env",
        ) 

        kernel_planckster = _setup_kernel_planckster(job_id)

        # Check protocol and setup the MinIO Repository if using s3
        # s3 by default
        logger.info(f"{job_id}: Checking storage protocol.")
        protocol = Protocol(os.getenv("STORAGE_PROTOCOL", Protocol.S3.value))

        if protocol not in [Protocol.S3, Protocol.LOCAL]:
            logger.error(f"{job_id}: STORAGE_PROTOCOL must be either 's3' or 'local'.")
            raise ValueError("STORAGE_PROTOCOL must be either 's3' or 'local'.")

        logger.info(f"{job_id}: Storage protocol: {protocol}")

        minio_repository = None
        if protocol == Protocol.S3:
            minio_repository = _setup_minio_repository(job_id)

        telegram_client = _setup_telegram_client(job_id)

        return kernel_planckster, protocol, minio_repository, telegram_client

    except Exception as error:
        logger.error(f"{job_id}: Unable to setup. Error:\n{error}")
        raise error


async def _scrape(
    job_id: int,
    channel_name: str,
    tracer_id: str,
    kernel_planckster: KernelPlancksterGateway,
    protocol: Protocol,
    minio_repository: MinIORepository | None,
    telegram_client: TelegramClient,
) -> JobOutput:


    try:

        output_lfns: List[LFN] = []
        async with telegram_client as client:

            # Set the job state to running
            logger.info(f"{job_id}: Starting Job")
            job_state = BaseJobState.RUNNING
            #job.touch()

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

                        if hasattr(message.media, "photo") and message.media.photo is not None:

                            # Download photo
                            with tempfile.NamedTemporaryFile() as tmp:
                                logger.info(
                                    f"{job_id}: Downloading photo to {tmp.name}"
                                )
                                file_location = await client.download_media(
                                    message.media.photo,
                                    file=tmp.name,
                                )

                                logger.info(
                                    f"{job_id}: Downloaded photo: {file_location}"
                                )
                                media_lfn: LFN = LFN(
                                    protocol=protocol,
                                    tracer_id=tracer_id,
                                    job_id=job_id,
                                    source=DataSource.TELEGRAM,
                                    relative_path=f"photos",
                                )
                                current_lfn = media_lfn

                                if protocol == Protocol.S3:
                                    pfn = minio_repository.lfn_to_pfn(media_lfn)
                                    logger.info(
                                        f"{job_id}:Uploading photo {media_lfn} to {pfn}"
                                    )
                                    minio_repository.upload_file(media_lfn, tmp.name)
                                    logger.info(
                                        f"{job_id}: Uploaded photo {media_lfn} to {pfn}"
                                    )

                                elif protocol == Protocol.LOCAL:
                                    file_name = f"data/{media_lfn.tracer_id}/{media_lfn.source.value}/{media_lfn.job_id}/{media_lfn.relative_path}"
                                    logger.info(
                                        f" {job_id}:Saving photo {media_lfn} locally to {file_name}"
                                    )
                                    os.makedirs(os.path.dirname(file_name), exist_ok=True)
                                    shutil.copy(tmp.name, file_name)
                                    logger.info(
                                        f"{job_id}: Saved photo {media_lfn} to {file_name}"
                                    )
                                    pfn = f"local://{file_name}"

                                output_lfns.append(media_lfn)
                                #job.touch()
                                kernel_planckster.register_new_data(
                                    pfns=[
                                        pfn,  # TODO: think if this is correct for kernel_planckster
                                    ],
                                )
                                last_successful_lfn = media_lfn

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
                                document_lfn: LFN = LFN(
                                    protocol=protocol,
                                    tracer_id=tracer_id,
                                    job_id=job_id,
                                    source=DataSource.TELEGRAM,
                                    relative_path="videos",
                                )
                                current_lfn = document_lfn

                                if protocol == Protocol.S3:
                                    pfn = minio_repository.lfn_to_pfn(document_lfn)
                                    logger.info(
                                        f" {job_id}: Uploading video {document_lfn} to {pfn}"
                                    )

                                    minio_repository.upload_file(
                                        document_lfn,
                                        file_location,
                                    )
                                    logger.info(
                                        f"{job_id}: Uploaded video {document_lfn} to {pfn}"
                                    )

                                elif protocol == Protocol.LOCAL:
                                    file_name = f"data/{document_lfn.tracer_id}/{document_lfn.source.value}/{document_lfn.job_id}/{document_lfn.relative_path}"
                                    logger.debug(
                                        f"{job_id}: Saving video {document_lfn} locally to {file_name}"
                                    )
                                    os.makedirs(os.path.dirname(file_name), exist_ok=True)
                                    shutil.copy(tmp.name, file_name)
                                    logger.info(
                                        f"{job_id}: Saved video {document_lfn} to {file_name}"
                                    )
                                    pfn = f"local://{file_name}"

                                output_lfns.append(document_lfn)
                                #job.touch()
                                kernel_planckster.register_new_data(
                                    pfns=[
                                        pfn,  # TODO: think if this is correct for kernel_planckster
                                    ],
                                )
                                last_successful_lfn = document_lfn

            except Exception as error:
                job_state = BaseJobState.FAILED
                logger.error(
                    f"{job_id}: Unable to scrape data. Error:\n{error}\nJob with tracer_id {tracer_id} failed.\nLast successful LFN: {last_successful_lfn}\nCurrent LFN: \"{current_lfn}\", job_state: \"{job_state}\""
                )
                #job.messages.append(f"Status: FAILED. Unable to scrape data. {error}")  # type: ignore
                #job.touch()
                # continue to scrape data if possible

            # Save the data to a CSV file
            logger.info(f"{job_id}: Saving data to CSV file")
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
                    tracer_id=tracer_id,
                    job_id=job_id,
                    source=DataSource.TELEGRAM,
                    relative_path="data.csv",
                )

                if protocol == Protocol.LOCAL:

                    pfn = f"data/{outfile_lfn.tracer_id}/{outfile_lfn.source.value}/{outfile_lfn.job_id}/{outfile_lfn.relative_path}"
                    # create directory if it does not exist
                    os.makedirs(os.path.dirname(pfn), exist_ok=True)
                    df.to_csv(pfn, encoding="utf-8")
                    logger.info(f"{job_id}: Saved data to {pfn}")

                elif protocol == Protocol.S3:

                    with tempfile.NamedTemporaryFile() as tmp:

                        df.to_csv(tmp.name, encoding="utf-8")

                        minio_repository.upload_file(
                            lfn=outfile_lfn,
                            file_path=tmp.name,
                        )

                        pfn = minio_repository.lfn_to_pfn(outfile_lfn)
                        logger.info(f"{job_id}: Uploaded data to {pfn}")

                else:
                    raise ValueError(f"Protocol {protocol} is not supported.")


                output_lfns.append(outfile_lfn)
                job_state = BaseJobState.FINISHED
                #job.touch()
                logger.info(f"{job_id}: Job finished")

                return JobOutput(
                    job_state=job_state,
                    tracer_id=tracer_id,
                    lfns=output_lfns,
                )

            except Exception as error:
                logger.error(
                    f"{job_id}: Unable to save data to CSV file. Job with tracer_id {tracer_id} failed. Error:\n{error}"
                )
                job_state = BaseJobState.FAILED
                #job.messages.append("Status: FAILED. Unable to save data to CSV file. ")
                #job.touch()

    except Exception as error:
        logger.error(f"{job_id}: Unable to scrape data. Job with tracer_id {tracer_id} failed. Error:\n{error}")
        job_state = BaseJobState.FAILED
        #job.messages.append(f"Status: FAILED. Unable to scrape data. {e}")


def main(
    job_id: int,
    channel_name: str,
    tracer_id: str,
    log_level: str = "WARNING",
) -> None:


    logging.basicConfig(level=log_level)

    if not all([job_id, channel_name, tracer_id]):
        logger.error(f"{job_id}: job_id, tracer_id, and channel_name must all be set.")
        raise ValueError("job_id, tracer_id, and channel_name must all be set.")


    kernel_planckster, protocol, minio_repository, telegram_client = _setup(job_id)

    if protocol == Protocol.S3 and minio_repository is None:
        raise ValueError("MinIORepository must be set when using the s3 protocol.")

    import asyncio

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        _scrape(
            job_id=job_id,
            channel_name=channel_name,
            tracer_id=tracer_id,
            kernel_planckster=kernel_planckster,
            protocol=protocol,
            minio_repository=minio_repository,
            telegram_client=telegram_client,
        )
    )

    loop.close()


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Scrape data from a telegram channel.")

    parser.add_argument(
        "--job-id",
        type=str,
        default="1",
        help="The job id",
    )

    parser.add_argument(
        "--channel-name",
        type=str,
        default="GCC_report",
        help="The channel name",
    )

    parser.add_argument(
        "--tracer-id",
        type=str,
        default="1",
        help="The tracer id",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="WARNING",
        help="The log level to use when running the scraper. Possible values are DEBUG, INFO, WARNING, ERROR, CRITICAL. Set to WARNING by default.",
    )

    args = parser.parse_args()


    main(
        job_id=args.job_id,
        channel_name=args.channel_name,
        tracer_id=args.tracer_id,
        log_level=args.log_level,
    )


