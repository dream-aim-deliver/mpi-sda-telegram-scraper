from logging import Logger
import os
from telethon import TelegramClient


def get_scraping_client(
    job_id: int,
    logger: Logger,
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
