from prefect import task, flow
from prefect.logging import get_run_logger

from embedder import create_qdrant_session


@task(name="retrieve")
def retrieve(query: str):
    logger = get_run_logger()
    qclient, vector_store, embeddings = create_qdrant_session()
    vectors = embeddings.embed_query(query)

    results = vector_store.similarity_search_by_vector(vectors)
    if results:
        for doc in results:
            logger.info(doc)

@flow(name="retrival", log_prints=True)
def retrival(query: str):
    ans = retrieve(query)
    print(ans)

if __name__ == "__main__":
    retrival.serve(
        name="retrival"
    )
