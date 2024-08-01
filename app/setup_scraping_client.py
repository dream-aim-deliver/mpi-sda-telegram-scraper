from logging import Logger
from telethon import TelegramClient


def get_scraping_client(
    job_id: int,
    logger: Logger,
    telegram_api_id: str,
    telegram_api_hash: str,
    telegram_phone_number: str | None = None,
    telegram_password: str | None = None,
    telegram_bot_token: str | None = None,
) -> TelegramClient:
    try:

        logger.info(f"{job_id}: Setting up Telegram client.")

        if not all([telegram_api_id, telegram_api_hash]):
            logger.error(
                f"{job_id}: telegram_api_id and telegram_api_hash must both be passed in."
            )
            raise ValueError("telegram_api_id and telegram_api_hash must both be passed in.")

        client = TelegramClient(
            "sda-telegram-scraper", telegram_api_id, telegram_api_hash
        )

        if telegram_phone_number:

            if not telegram_password:
                logger.info(f"{job_id}: Starting Telegram client with phone number")
                client.start(
                    phone=telegram_phone_number,
                )

            else:
                logger.info(
                    f"{job_id}: Starting Telegram client with phone number and password"
                )
                client.start(
                    phone=telegram_phone_number,
                    password=telegram_password,
                )

        elif telegram_bot_token:
            logger.info(f"{job_id}: Starting Telegram client with bot token")
            client.start(
                bot_token=telegram_bot_token,
            )

        else:
            logger.info(f"{job_id}: Starting Telegram client...")
            client.start()


        logger.info(f"{job_id}: Telegram client started successfully")

        return client

    except Exception as error:
        logger.error(f"{job_id}: Unable to setup the Telegram client. Error:\n{error}")
        raise error
