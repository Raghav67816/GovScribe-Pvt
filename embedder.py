from os import getenv
from dotenv import load_dotenv

from prefect import task, flow
from qdrant_client import QdrantClient
