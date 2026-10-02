import random

from dotenv import load_dotenv
from os import getenv, getxattr

from prefect import task, flow
from prefect.logging import get_run_logger

from langchain_core.documents import Document
from langchain_qdrant import QdrantVectorStore
from langchain_nomic.embeddings import NomicEmbeddings
from langchain_experimental.text_splitter import SemanticChunker

from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, PointStruct, Distance

load_dotenv(".env")

connection_url = getenv("QDRANT_URL")

print(connection_url)

collection_name = getenv("QDRANT_COLLECTION_NAME")

def create_qdrant_session():
    qclient = QdrantClient(url=connection_url)

    embeddings = NomicEmbeddings(
        model="nomic-embed-text-v1.5",
        dimensionality=768,
        inference_mode="local"
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

    vector_store = QdrantVectorStore(
        client=qclient,
        embedding=embeddings,
        collection_name=collection_name,
    )

    return (qclient, vector_store, embeddings)


@task(name="text-chunking")
def text_chunking(content: str) -> list:

    logger = get_run_logger()
    qclient, vector_store, embeddings = create_qdrant_session()

    splitter = SemanticChunker(
        embeddings,
        breakpoint_threshold_type="percentile",
        breakpoint_threshold_amount=90.0
    )

    chunks = splitter.create_documents([content])
    logger.info(f"created {len(chunks)} documents")

    return chunks



@task(name="ammendment-check")
def check_ammendment(chunks: list) -> bool:
    logger = get_run_logger()
    qclient, vector_store, embeddings = create_qdrant_session()

    match_count = 0
    matching_payload = []

    chunks = [chunk.page_content for chunk in chunks]

    if len(chunks) == 0 or not qclient.collection_exists(collection_name):
        logger.error("collection does not exists... skipping checks")
        return False

    # TODO: check similary search
    num = random.randint(4, 11)
    for i in range(0, num):
        random_chunk_idx = random.randint(0, len(chunks) - 1)
        logger.info(f"random chunk index: {random_chunk_idx}")

        query_text = f"search_query: {chunks[random_chunk_idx]}"

        query_vector = embeddings.embed_query(query_text)

        results = qclient.query_points(
            collection_name,
            query=query_vector,
            limit=1,
            score_threshold=0.85,
            with_payload=True
        )

        if len(results.points) > 0:
            match_count += 1
            for point in results.points:
                logger.info(point.id)
                logger.info(point.score)

    ratio = match_count / num

    if ratio >= 0.50:
        logger.info(f"ratio: {ratio} matched, ammendment detected")
        return True

    else:
        return False

@task(name="embed-chunks")
def embed_chunks(chunks: list[Document]):

    logger = get_run_logger()

    qclient, vector_store, embeddings = create_qdrant_session()
    try:
        vector_store.add_texts([chunk.page_content for chunk in chunks], [])
        logger.info(f"embedded {len(chunks)}")

    except Exception as error:
        logger.error(f"failed to embed: {str(error)}")

@task(name="query", log_prints=True)
def query(que: str):
    logger = get_run_logger()
    qclient, vector_store, embeddings = create_qdrant_session()

    vectors = embeddings.embed_query(que)
    results = vector_store.similarity_search_by_vector(vectors)

    for doc in results:
        print(doc)
        logger.info(doc.page_content)
        

@flow(name="embedder", log_prints=True)
def embedder(content: str):
    chunks = text_chunking(content)
    is_ammended = check_ammendment(chunks)
    embed_chunks(chunks)

if __name__ == "__main__":
    embedder.serve(
        name="embedder"
    )

