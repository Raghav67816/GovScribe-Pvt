from os import getenv
from dotenv import load_dotenv

from sqlmodel import Session
from sqlalchemy import create_engine


try:
    load_dotenv(".env")
    print("env file loaded")

except Exception as error:
    print(f"failed to load env file: {str(error)}")


def get_db_session() -> Session:
    engine = create_engine(
        url=getenv("DB_URL"),
        pool_size=10,
        max_overflow=20
    )

    return Session(engine)
