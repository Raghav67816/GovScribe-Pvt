from os import getenv
from dotenv import load_dotenv

from prefect import task, flow

from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, PointStruct, Distance

load_dotenv(".env")

connection_url = getenv("QDRANT_URL")
collection_name = getenv("documents")

qclient = QdrantClient(url=connection_url)


if not qclient.collection_exists(collection_name):
    qclient.create_collection(
        collection_name,
        vectors_config=VectorParams(
            size=768,
            distance=Distance.COSINE,
            on_disk=True
        )
    )

@task(name="ammendment-check")
def check_ammendment(chunks: list[dict]):
    pass
