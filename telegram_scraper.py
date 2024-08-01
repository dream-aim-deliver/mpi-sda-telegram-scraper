import logging
from app.scraper import scrape
from app.sdk.models import KernelPlancksterSourceData, BaseJobState
from app.sdk.scraped_data_repository import ScrapedDataRepository
from app.setup import setup


from app.setup_scraping_client import get_scraping_client


def main(
    job_id: int,
    channel_name: str,
    tracer_id: str,
    kp_auth_token: str,
    kp_host: str,
    kp_port: int,
    kp_scheme: str,
    telegram_api_id: str,
    telegram_api_hash: str,
    openai_api_key: str,
    telegram_phone_number: str | None = None,
    telegram_password: str | None = None,
    telegram_bot_token: str | None = None,
    log_level: str = "WARNING",
) -> None:

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=log_level)

    if not all([job_id, channel_name, tracer_id]):
        logger.error(f"{job_id}: job_id, tracer_id, and channel_name must all be set.")
        raise ValueError("job_id, tracer_id, and channel_name must all be set.")

    kernel_planckster, protocol, file_repository = setup(
        job_id=job_id,
        logger=logger,
        kp_auth_token=kp_auth_token,
        kp_host=kp_host,
        kp_port=kp_port,
        kp_scheme=kp_scheme,
    )

    scraped_data_repository = ScrapedDataRepository(
        protocol=protocol,
        kernel_planckster=kernel_planckster,
        file_repository=file_repository,
    )

    telegram_client = get_scraping_client(
        job_id=job_id,
        logger=logger,
        telegram_api_id=telegram_api_id,
        telegram_api_hash=telegram_api_hash,
        telegram_phone_number=telegram_phone_number,
        telegram_password=telegram_password,
        telegram_bot_token=telegram_bot_token,
    )

    import asyncio

    loop = asyncio.get_event_loop()

    loop.run_until_complete(
        scrape(
            job_id=job_id,
            channel_name=channel_name,
            tracer_id=tracer_id,
            scraped_data_repository=scraped_data_repository,
            telegram_client=telegram_client,
            log_level=log_level,
            openai_api_key=openai_api_key,
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

    parser.add_argument(
        "--kp-auth-token",
        type=str,
        default="",
        help="The Kernel Planckster auth token",
    )

    parser.add_argument(
        "--kp-host",
        type=str,
        default="localhost",
        help="The Kernel Planckster host",
    )

    parser.add_argument(
        "--kp-port",
        type=int,
        default=8000,
        help="The Kernel Planckster port",
    )

    parser.add_argument(
        "--kp-scheme",
        type=str,
        default="http",
        help="The Kernel Planckster scheme",
    )

    parser.add_argument(
        "--telegram-api-id",
        type=str,
        default="",
        help="The Telegram API ID",
    )

    parser.add_argument(
        "--telegram-api-hash",
        type=str,
        default="",
        help="The Telegram API Hash",
    )

    parser.add_argument(
        "--telegram-phone-number",
        type=str,
        default="",
        help="The Telegram phone number",
    )

    parser.add_argument(
        "--telegram-password",
        type=str,
        default="",
        help="The Telegram password",
    )

    parser.add_argument(
        "--telegram-bot-token",
        type=str,
        default="",
        help="The Telegram bot token",
    )

    parser.add_argument(
        "--openai-api-key",
        type=str,
        default="",
        help="The OpenAI API Key",
    )

    args = parser.parse_args()

    main(
        job_id=args.job_id,
        channel_name=args.channel_name,
        tracer_id=args.tracer_id,
        log_level=args.log_level,
        kp_auth_token=args.kp_auth_token,
        kp_host=args.kp_host,
        kp_port=args.kp_port,
        kp_scheme=args.kp_scheme,
        telegram_api_id=args.telegram_api_id,
        telegram_api_hash=args.telegram_api_hash,
        telegram_phone_number=args.telegram_phone_number,
        telegram_password=args.telegram_password,
        telegram_bot_token=args.telegram_bot_token,
        openai_api_key=args.openai_api_key,
    )
