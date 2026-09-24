import logging
from os import getenv

from dotenv import load_dotenv
from sqlalchemy import create_engine
from hatchet_sdk import Hatchet, ClientConfig

# load .env config
try:
    load_dotenv("./.env")

except Exception:
    print("failed to load environment")


# hatchet shared client
logging.basicConfig(level=logging.INFO)
root_logger = logging.getLogger()


hatchet_client = Hatchet(
    config=ClientConfig(
        logger=root_logger
    )
)
psql_engine = create_engine(getenv("DATABASE_URL"))

