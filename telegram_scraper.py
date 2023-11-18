import os
import logging
import pandas as pd # type: ignore
# import nest_asyncio
from dotenv import load_dotenv
from telethon.sync import TelegramClient # type: ignore
from app.sdk.models import LFN, BaseJobState, DataSource, Protocol

from app.telegram_scraper_impl import TelegramScraperJob

logger = logging.getLogger(__name__)

# Initialize nest_asyncio to run asyncio within Jupyter/IPython
# nest_asyncio.apply()

# TODO logger

async def scrape(job: TelegramScraperJob, channel_name: str, api_id: str, api_hash: str):
    try:
        async with TelegramClient(
            'sda-telegram-scraper', api_id, api_hash
        ) as client:
            outfile_lfn: LFN = LFN(
                protocol=Protocol.LOCAL,
                tracer_id=job.tracer_id,
                job_id=job.id,
                source=DataSource.TELEGRAM,
                relative_path="data2_climate.csv",
            )
            logger.info(f"Starting Job {job}")
            outfile = outfile_lfn.pfn
            logger.info(f"Output will be saved to: {outfile}")
            job.output_lfns.append(outfile_lfn) 
            job.state = BaseJobState.RUNNING
            job.touch()
            
            data = []
            try:
                async for message in client.iter_messages(f"https://t.me/{channel_name}"):
                    print(message.sender_id, ":", message.text, message.date)
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
                            media_lfn: LFN = LFN(
                                protocol=Protocol.LOCAL,
                                tracer_id=job.tracer_id,
                                job_id=job.id,
                                source=DataSource.TELEGRAM,
                                relative_path=file_location,
                            )
                            
                            # Download photo
                            file_location = await client.download_media(
                                message.media.photo,
                                file=os.path.join("downloaded_media", "photos"),
                            )
                                                          
                            
                            print(f"Downloaded photo: {file_location}")
                        elif hasattr(message.media, "document"):
                            # Download video (or other documents)
                            file_location = await client.download_media(
                                message.media.document,
                                file=os.path.join("downloaded_media", "videos"),
                            )
                            print(f"Downloaded video: {file_location}")
            except Exception as error:
                job.state = BaseJobState.FAILED
                job.messages.append(f"Status: FAILED. Unable to scrape data. {error}") # type: ignore
                 

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

    except Exception as e:
        job.state = BaseJobState.FAILED
        job.messages.append(f"Status: FAILED. Unable to scrape data. {e}")

