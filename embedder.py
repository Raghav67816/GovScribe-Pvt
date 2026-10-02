import random

from dotenv import load_dotenv
from os import getenv, getxattr

from prefect import task, flow
from prefect.logging import get_run_logger

from langchain_qdrant import QdrantVectorStore
from langchain_nomic.embeddings import NomicEmbeddings
from langchain_experimental.text_splitter import SemanticChunker

from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, PointStruct, Distance

load_dotenv(".env")

connection_url = getenv("QDRANT_URL")
collection_name = getenv("documents")

qclient = QdrantClient(url=connection_url)

embeddings = NomicEmbeddings(
    model="nomic-embed-text-v1.5",
    dimensionality=512
)

vector_store = QdrantVectorStore(
    client=qclient,
    embedding=embeddings,
    collection_name=collection_name
)


if not qclient.collection_exists(collection_name):
    qclient.create_collection(
        collection_name,
        vectors_config=VectorParams(
            size=768,
            distance=Distance.COSINE,
            on_disk=True
        )
    )

@task(name="text-chunking")
def text_chunking(content: str, purpose: str) -> list:

    logger = get_run_logger()

    valid_purpose = ["search_document", "search_query"]

    if purpose not in valid_purpose:
        logger.warning(f"purpose: {purpose} is not valid")
        return []

    splitter = SemanticChunker(
        embeddings,
        breakpoint_threshold_type="percentile",
        breakpoint_threshold_amount=90.0
    )

    chunks = splitter.create_documents([content])
    logger.info(f"created {len(chunks)} documents")

    return [chunk.page_content for chunk in chunks]



@task(name="ammendment-check")
def check_ammendment(chunks: list) -> bool:
    logger = get_run_logger()

    match_count = 0
    matching_payload = []

    if len(chunks) == 0 or not qclient.collection_exists(collection_name):
        logger.error("collection does not exists... skipping checks")
        return False

    num = random.randint(4, 11)
    for i in range(0, num):
        random_chunk_idx = random.randint(0, len(chunks))

        query_text = f"search_query: {chunks[random_chunk_idx]}"
        query_vector = embeddings.embed_query(query_text)

        results = qclient.query_points(
            collection_name,
            query=query_vector,
            limit=1,
            score_threshold=0.85,
            with_payload=True
        )

        if results:
            match_count += 1
            matching_payload.append(results[0].payload)

    ratio = match_count / num

    if ratio >= 0.50:
        logger.info(f"ratio: {ratio} matched, ammendment detected")
        return True

    else:
        return False


@task(name="embedd-document")
def embed_document(chunks: list, path: str):
    logger = get_run_logger()
    logger.info(f"embedding {len(chunks)} from {path}")

    m_chunks = [f"search_document: {chunk}" for chunk in chunks]
    vector_embeding = embeddings.embed_documents(m_chunks)

    payload = {
        "url": getxattr(path, "user.url").decode('utf-8'),
        "date_created": getxattr(path, "user.doc").decode('utf-8'),
        "date_modified": getxattr(path, "user.dom").decode("utf-8")
    }

    vector_store.add_documents(m_chunks)
