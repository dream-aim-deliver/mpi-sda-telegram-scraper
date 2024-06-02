from logging import Logger
import logging
import os
import tempfile
from typing import List
from telethon import TelegramClient
from app.sdk.models import KernelPlancksterSourceData, BaseJobState, JobOutput
from app.sdk.scraped_data_repository import ScrapedDataRepository
from pydantic import BaseModel
from typing import Literal
import instructor
from instructor import Instructor
from openai import OpenAI
from geopy.geocoders import Nominatim
import pandas as pd


class messageData(BaseModel):
    city: str
    country: str
    year: int
    month: Literal[
        "January",
        "Febuary",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    day: Literal[
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12",
        "13",
        "14",
        "15",
        "16",
        "17",
        "18",
        "19",
        "20",
        "21",
        "22",
        "23",
        "24",
        "25",
        "26",
        "27",
        "28",
        "29",
        "30",
        "31",
    ]
    disaster_type: Literal["Wildfire", "Climate", "Other"]


# Potential alternate prompting
# class messageDataAlternate(BaseModel):
#     city: str
#     country: str
#     year: int
#     month: Literal['January', 'Febuary', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December', 'Unsure']
#     day: Literal['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', 'Unsure']
#     disaster_type: Literal[topic, 'Other']


class filterData(BaseModel):
    relevant: bool


class TwitterScrapeRequestModel(BaseModel):
    query: str
    outfile: str
    api_key: str


async def scrape(
    job_id: int,
    channel_name: str,
    tracer_id: str,
    scraped_data_repository: ScrapedDataRepository,
    telegram_client: TelegramClient,
    openai_api_key: str,
    topic: str,
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
            # job.touch()

            data = []
            augmented_data = []
            filter = topic
            # Enables `response_model`
            instructor_client = instructor.from_openai(OpenAI(api_key=openai_api_key))

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
                    if message.text:
                        augmented_data.append(
                            augment_telegram(instructor_client, message, filter)
                        )

                    # Check if the message has media (photo or video)
                    if message.media:

                        if (
                            hasattr(message.media, "photo")
                            and message.media.photo is not None
                        ):

                            # Download photo
                            with tempfile.NamedTemporaryFile() as tmp:
                                logger.info(
                                    f"{job_id}: Downloading photo to {tmp.name}"
                                )
                                file_location = await client.download_media(
                                    message.media.photo, file=tmp.name
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
                                # job.touch()

                                last_successful_data = media_data

                        elif (
                            hasattr(message.media, "document")
                            and message.media.document is not None
                        ):

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
                                # job.touch()
                                last_successful_data = document_data

                with tempfile.NamedTemporaryFile() as tmp:
                    print(augmented_data)
                    if augmented_data:
                        df = pd.DataFrame(
                            augmented_data,
                            columns=[
                                "Title",
                                "Telegram",
                                "Extracted_Location",
                                "Resolved_Latitude",
                                "Resolved_Longitude",
                                "Month",
                                "Day",
                                "Year",
                                "Disaster_Type",
                            ],
                        )
                        file_name = f"{os.path.basename(tmp.name)}"
                        df.to_json(
                            f"{tmp.name}",
                            orient="index",
                            indent=4,
                        )

                        final_augmented_data = KernelPlancksterSourceData(
                            name=f"telegram_all_augmented",
                            protocol=protocol,
                            relative_path=f"telegram/{tracer_id}/{job_id}/augmented/data.json",
                        )
                        try:
                            scraped_data_repository.register_scraped_json(
                                final_augmented_data,
                                job_id,
                                f"{tmp.name}",
                            )
                        except Exception as e:
                            logger.info("could not register file")

            except Exception as error:
                job_state = BaseJobState.FAILED
                logger.error(
                    f'{job_id}: Unable to scrape data. Error:\n{error}\nJob with tracer_id {tracer_id} failed.\nLast successful data: {last_successful_data}\nCurrent data: "{current_data}", job_state: "{job_state}"'
                )
                # job.messages.append(f"Status: FAILED. Unable to scrape data. {error}")  # type: ignore
                # job.touch()

                # continue to scrape data if possible

            job_state = BaseJobState.FINISHED
            # job.touch()
            logger.info(f"{job_id}: Job finished")
            return JobOutput(
                job_state=job_state,
                tracer_id=tracer_id,
                source_data_list=output_data_list,
            )

    except Exception as error:
        logger.error(
            f"{job_id}: Unable to scrape data. Job with tracer_id {tracer_id} failed. Error:\n{error}"
        )
        job_state = BaseJobState.FAILED

        # job.messages.append(f"Status: FAILED. Unable to scrape data. {e}")

def augment_telegram(client: Instructor, message: any, filter: str):
    if len(message.text) > 5:
        # extract aspects of the tweet
        title = message.peer_id.channel_id
        content = message.text

        # Relevancy filter with gpt-4
        filter_data = client.chat.completions.create(
            model="gpt-4",
            response_model=filterData,
            messages=[
                {
                    "role": "user",
                    "content": f"Examine this telegram message: {content}. Is this telegram message describing {filter}? ",
                },
            ],
        )

    if filter_data.relevant:
        aug_data = None
        try:
            # Location extraction with GPT-4 Turbo
            aug_data = client.chat.completions.create(
                model="gpt-4-turbo",
                response_model=messageData,
                messages=[
                    {"role": "user", "content": f"Extract: {content}"},
                ],
            )
        except Exception as e:
            Logger.error(f"Could not augment message with initial prompt: {e}")
            try:
                # Alternative prompt
                aug_data = client.chat.completions.create(
                    model="gpt-4-turbo",
                    response_model=messageData,
                    messages=[
                        {"role": "user", "content": f"can we extract any information about {filter} from this message {content}?"},
                    ],
                )
            except Exception as e2:
                Logger.error(f"Could not augment message with alternative prompt: {e2}")
                return None

        if aug_data:                                                        
            city = getattr(aug_data, 'city', 'unknown city')
            country = getattr(aug_data, 'country', 'unknown country')
            extracted_location = f"{city},{country}"
            year = getattr(aug_data, 'year', 'unknown year')
            month = getattr(aug_data, 'month', 'unknown month')
            day = getattr(aug_data, 'day', 'unknown day')
            disaster_type = getattr(aug_data, 'disaster_type', 'Other')

            # NLP-informed geolocation
            try:
                coordinates = get_lat_long(extracted_location)
                latitude, longitude = coordinates if coordinates else ("no latitude", "no longitude")
            except Exception as e:
                Logger.error(f"Geolocation error: {e}")
                latitude, longitude = "no latitude", "no longitude"

            return [
                title,
                content,
                extracted_location,
                latitude,
                longitude,
                month,
                day,
                year,
                disaster_type,
            ]
    else:
        return [
            title,
            content,
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "Other",
        ]

     


# utility function for augmenting tweets with geolocation
def get_lat_long(location_name):
    geolocator = Nominatim(user_agent="location_to_lat_long")
    try:
        location = geolocator.geocode(location_name)
        if location:
            latitude = location.latitude
            longitude = location.longitude
            return latitude, longitude
        else:
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None
