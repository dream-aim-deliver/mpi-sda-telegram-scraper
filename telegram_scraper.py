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
    )

    scraped_data_repository = ScrapedDataRepository(
        protocol=protocol,
        kernel_planckster=kernel_planckster,
        file_repository=file_repository,
    )

    telegram_client = get_scraping_client(
        job_id=job_id,
        logger=logger,
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


