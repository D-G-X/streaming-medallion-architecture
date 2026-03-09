import os
from dotenv import load_dotenv

load_dotenv()

COINGECKO_API_URL = os.getenv(
    "COINGECKO_API_URL",
    "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
)
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL_SECONDS", "300"))
MAX_RETRIES = 3
RETRY_DELAY = 5
