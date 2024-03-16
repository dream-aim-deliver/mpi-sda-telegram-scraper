## Use

You can start the application by pulling the docker image from Dockerhub.
You will need to provide the required environment variables, and mount the session file to the container. The best approach would be to create your own `.env` file, following `.env.template`, and load it into the container:

```bash
docker run --rm \
    --name mpi-telegram-scraper \
    -v "${PWD}/sda-telgram-scraper.session:/telegram_scaper/sda-telegram-scraper.session:ro" \
    -v "${PWD}/.env:/app/.env:ro" \
    --net="host" \
    mpi-telegram-scraper
```

See the [Development](#development) section for more information on the required environment variables.
Now you can run the main scraper script with the following command.
All parameters have the default values stated below:

```bash
docker exec -it mpi-telegram-scraper python3 telegram_scraper.py --log-level=WARNING --job_id=1 --tracer_id="1" --channel_name="sda_test"
```

Change `--log-level` to `INFO` to see more detailed logs.

When executing the `telegram_scraper.py` script inside the container, if everything is set up correctly, the Telegram client will send a verification code to the phone number you provided. You will need to enter this code in the terminal to continue.


## Development

### Setup and Environment Variables

1. Install the required packages, preferably in a virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. If you wish to test the scraper with a minIO instance, you can start a test minIO container using the docker-compose file in the root directory
```bash
docker compose -f minio-docker-compose.yml up -d
```

3. Start a kernel-planckster instance:
    - Clone the [kernel-planckster](https://github.com/dream-aim-deliver/kernel-planckster) repo elsewhere
    - Install the required packages, preferable in its own virtual environment, following the instructions in the README
    - Run it in dev mode following the README, where you'll find the host and port

4. Obtain the following credentials from [Telegram](https://core.telegram.org/api/obtaining_api_id): api ID, and api hash. You will also need the phone number and a password of the account you want to use for scraping. **IMPORTANT**: You will need access to the phone you provided, as Telegram will send a verification code to it.

5. Copy the `.env.example` file to `.env` and fill in the required fields.
    - For `KERNEL_PLANCKSTER_*`, get them from the kernel-planckster README or the instance you started in step 3
    - You can choose the storage protocol to use:
        + `s3` for minIO, will require the `MINIO_*` fields that you can get, for example, from the `minio-docker-compose.yml` file
        + `local` for local storage, will create a `data` directory in the root of the project and store the files there
    - The `TELEGRAM_*` fields are the credentials you obtained in step 4


### Standalone Execution

After doing the setup, you can now execute the main scraper script. All parameters are optional, and below are the default values:
```bash
python3 telegram_scraper.py --log-level=WARNING --job_id=1 --tracer_id="1" --channel_name="sda_test"
```

If everything is set up correctly, the Telegram client will send a verification code to the phone number you provided. You will need to enter this code in the terminal to continue.
This configuration will be stored in a file called `sda-telegram-scraper.session` in the root of the project. This file will be used to authenticate the Telegram client in future runs, so you won't need to enter the verification code again.


### Build Image

You can dockerize the application by building an image with the following command.
Make sure to fill in the `.env` file with the required credentials, by following the `.env.template` file and the [Setup and Environment Variables](#setup-and-environment-variables) section:

```bash
docker build -t mpi-telegram-scraper .
# or, if using buildx:
docker build --load -t mpi-telegram-scraper .
```

Then you can do:

```bash
docker run --rm \
    --name mpi-telegram-scraper \
    -v "${PWD}/sda-telgram-scraper.session:/telegram_scaper/sda-telegram-scraper.session:ro" \
    -v "${PWD}/.env:/app/.env:ro" \
    --net="host" \
    mpi-telegram-scraper
```

And now, to run the main scraper script:

```bash
docker exec -it mpi-telegram-scraper python3 telegram_scraper.py --log-level=WARNING --job_id=1 --tracer_id="1" --channel_name="sda_test"
```

Change `--log-level` to `INFO` to see more detailed logs.