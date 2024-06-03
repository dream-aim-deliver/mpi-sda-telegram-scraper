## Use

You can start the application by pulling the docker image from Dockerhub.
You will need to provide the required command line arguments, and mount the session file to the container. 

```bash
docker run --rm \
    --name mpi-telegram-scraper \
    -v "${PWD}/sda-telegram-scraper.session:/telegram_scaper/sda-telegram-scraper.session:ro" \
    --net="host" \
    mpi-telegram-scraper
```

See the [Development](#development) section for more information on the command line arguments
Now you can run the main scraper script with the following command.
All parameters have the default values stated below:

```bash
docker exec -it mpi-telegram-scraper python3 telegram_scraper.py --log-level=WARNING --job-id=1 --tracer-id="1" --channel-name="GCC_report"
```

Change `--log-level` to `INFO` to see more detailed logs.

When executing the `telegram_scraper.py` script inside the container, if everything is set up correctly, the Telegram client will send a verification code to the phone number you provided. You will need to enter this code in the terminal to continue.


## Development

### Setup and CLI arguments

1. Install the required packages, preferably in a virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Start a kernel-planckster instance:
    - Clone the [kernel-planckster](https://github.com/dream-aim-deliver/kernel-planckster) repo elsewhere
    - Install the required packages, preferable in its own virtual environment, following the instructions in the README
    - Run it in dev mode with a object store following the README (e.g., `poetry run dev --storage`), where you'll find the host, port, auth key and schema.

3. Obtain the following credentials from [Telegram](https://core.telegram.org/api/obtaining_api_id): api ID, and api hash. You will also need the phone number and a password of the account you want to use for scraping. **IMPORTANT**: You will need access to the phone you provided, as Telegram will send a verification code to it.

4. Create a [.env] file containing the API_ID and API_HASH values,and then run the [generate-session.py] file.

5. You will pass the required parameters as command line arguments when running the application. The command line arguments should be modelled in a similar fashion as shown below (an example command to run a script with default values:)
```bash
python3 telegram_scraper.py --log-level=WARNING --job-id=1 --tracer-id="1" --channel-name="GCC_report" --telegram-api-id=API_ID_VALUE --telegram-api-hash=HASH_VALUE --telegram-phone-number=VALID_PHONE_NUMBER --telegram-password="testpassword" --openai-api-key=VALID_KEY
```

**Another method (OR)**

Open the [demo.sh] file and update the values of [--telegram-api-id] , [--telegram-api-hash] , [--telegram-phone-number],[--telegram-password] to match your credentials. Also add a valid [--openai-api-key] then run using-
```bash
./demo.sh
```


### Standalone Execution

After doing the setup, you can now execute the main scraper script. All parameters are optional, and below are the default values:
```bash
python3 telegram_scraper.py --log-level=WARNING --job-id=1 --tracer-id="1" --channel-name="GCC_report"
```

If everything is set up correctly, the Telegram client will send a verification code to the phone number you provided. You will need to enter this code in the terminal to continue.
This configuration will be stored in a file called `sda-telegram-scraper.session` in the root of the project. This file will be used to authenticate the Telegram client in future runs, so you won't need to enter the verification code again.


### Build Image

You can dockerize the application by building an image with the following command.
Make sure to run the commands with the required credentials, by following the command examples and the [Setup and CLI](#setup-and-CLI) section:

```bash
docker build -t mpi-telegram-scraper .
# or, if using buildx:
docker build --load -t mpi-telegram-scraper .
```

Then you can do:

```bash
docker run --rm \
    --name mpi-telegram-scraper \
    -v "${PWD}/sda-telegram-scraper.session:/telegram_scaper/sda-telegram-scraper.session:ro" \
    
    --net="host" \
    mpi-telegram-scraper \
```

And now, to run the main scraper script:

```bash
docker exec -it mpi-telegram-scraper python3 telegram_scraper.py --log-level=WARNING --job-id=1 --tracer-id="1" --channel-name="sda_test"
```

Change `--log-level` to `INFO` to see more detailed logs.