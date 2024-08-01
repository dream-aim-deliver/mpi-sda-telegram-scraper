# MPI Telegram Scraper





## Development

### Setup

1. Install the required packages, preferably in a virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. You will need a phone number and a Telegram account. Obtain the following credentials linked to your account, from [Telegram](https://core.telegram.org/api/obtaining_api_id): API ID, and API hash.
    i. **Note 1**: You will need access to the phone linked to the telegram account to which the API ID and hash belong, as Telegram will send a verification code to it the first time you run the scraper. The phone number must be in international format (e.g., +391234567890 for an Italian number).
    ii. **Note 2**: If your Telegram account is password protected, you will need to provide the password the first time you run the scraper.

3. Obtain an OpenAI API key from the [OpenAI website](https://openai.com/api/) and take note of it

4. Setup kernel-planckster locally:
    1. Clone the [kernel-planckster](https://github.com/dream-aim-deliver/kernel-planckster) repo elsewhere
    2. Install the required packages, preferable in its own virtual environment, following the instructions in the README
    3. Run it in dev mode with a object store following the README (e.g., `poetry run dev:storage`), where you'll find the host, port, auth key and schema
    4. Kernel Planckster's host, port, auth key and schema will be used as command line arguments when running the main scraper script, so take note of them


### Standalone Execution

After doing the setup, you can now execute the main scraper script. `log_level`, `job-id`, `tracer-id` and `channel` are optional. Below are the default values, used for testing purposes:

```bash
python telegram_scraper.py \
    --log-level=WARNING \
    --job-id=1 --tracer-id="1" \
    --channel-name="GCC_report" \
    --telegram-api-id="your_telegram_api_id" \
    --telegram-api-hash="your-telegram-api-hash" \
    --openai-api-key="your-openai-api-key" \
    --kp-host="localhost" --kp-port=8000 --kp-scheme="http" \
    --kp-auth-token="test123"
```

The first time you run this, if everything is set up correctly, the Telegram client will prompt you for a phone number.
Then, Telegram will send a verification code to the phone number provided, and will prompt you for it.
Additionally, if your Telegram account is password protected, you will also need to provide the password.

Afterwards, a file called `sda-telegram-scraper.session` will be created in the root of the project, which will be used to authenticate the Telegram client in future runs without needing to re-enter the verification code.
This .session file can be shared with other team members to avoid having to re-enter the verification code on their machines.

#### Minimizing Prompting with Optional Command line arguments

The following optional command line arguments can be passed to the main scraper script to minimize the need for manual intervention the first time the scraper is run:

- `--telegram-phone-number`: The phone number linked to the telegram account. Must be in international format (e.g., +391234567890 for an Italian number).
- `--telegram-password`: The password of the telegram account, if it is password protected.
- `--telegram-bot-token`: The token of the telegram bot that will be used to send messages to the channel.

If you pass either a phone number (with its corresponding password, if the account is password protected) or a bot token, the script will prompt you only for the verification code the first time you run it.
Then, it will create the .session file as explained above.

#### Avoiding Prompting with a Pre-Generated `.session` file

To avoid prompting completely the first time you run the scraper, you can pre-generate a `.session` file **in an interactive shell** by running:

```bash
API_ID="your-telegram-api-id" API_HASH="your-telegram-api-hash" python generate_session.py
```

This will prompt you for the phone number (and password, if the account is password protected). Then, Telegram will send a verification code to the phone number provided, and you will need to enter it in the terminal. The script will then authenticate the Telegram client and will create a `.session` file.

When the scraper is run, it will use this `.session` file to authenticate the Telegram client, avoiding the need to re-enter the verification code every time the scraper is run.


## Docker

The application can be dockerized for easier deployment and execution. Below are the steps to build and run the application in a Docker container.

1. Build the Docker image:

```bash
docker build -t mpi-telegram-scraper .
# or, if using buildx:
docker build --load -t mpi-telegram-scraper .
```

2. Then you can run a Docker container with the following command:

```bash
docker run --rm \
    --name mpi-telegram-scraper \
    --net="host" \
    mpi-telegram-scraper
```

Optionally, if you have a `.session` file already generated (either [pre-generated](#avoiding-prompting-with-a-pre-generated-session-file), or created by running the scraper script [interactively](#standalone-execution)), you can mount it to the container like so: 

```bash
docker run --rm \
    --name mpi-telegram-scraper \
    -v "${PWD}/sda-telegram-scraper.session:/app/sda-telegram-scraper.session" \
    --net="host" \
    mpi-telegram-scraper
```

3. You can now run the main scraper script with the following command:

```bash
docker exec -it mpi-telegram-scraper python3 telegram_scraper.py \
    --log-level=WARNING \
    --job-id=1 --tracer-id="1" \
    --channel-name="GCC_report" \
    --telegram-api-id="your_telegram_api_id" \
    --telegram-api-hash="your-telegram-api-hash" \
    --openai-api-key="your-openai-api-key" \
    --kp-host="localhost" --kp-port=8000 --kp-scheme="http" \
    --kp-auth-token="test123"
```

Change `--log-level` to `INFO` to see more detailed logs.
If you mounted the `.session` file, you will not be prompted when running the scraper script.


## Production

Note that, the first time that the scraper is run, the Telegram client might prompt you at least for a verification code. This is not ideal for production, as it requires manual intervention.

To avoid this, the preferred way to go is to generate a `.session` file as explained [above](#avoiding-prompting-with-a-pre-generated-session-file), **in an interactive shell**.
This file can then be put in your production environment, at the root of the project, to avoid being prompted when the scraper is run.
