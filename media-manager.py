from app import get_db_session

from curl_cffi.requests import get

from prefect import flow, task
from prefect.logging import get_run_logger

from pymupdf4llm import to_markdown

from os import makedirs, getcwd
from urllib.parse import urlparse
from os.path import basename, join

@task(name="download-document", retries=3, timeout_seconds=20)
def download_document(url: str) -> str:
    logger = get_run_logger()

    url_parsed = urlparse(url)
    filename = basename(url_parsed.path)

    output_folder = f"{getcwd()}/documents"
    path = join(output_folder, f"{filename}.pdf")
    makedirs(output_folder, exist_ok=True)

    response = get(
        url=url,
        stream=True,
        verify=False,
        impersonate="chrome"
    )

    if response.status_code == 200:
        with open(path, "wb") as doc:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    doc.write(chunk)

        logger.info(f"downloaded at: {path}")

        return path

    else:
        logger.error(f"server returned: {response.status_code}")
        return ""

@task(name="extract-text")
def extract_text(path: str):
    logger = get_run_logger()

    if path == "":
        logger.warning("path provided is empty")
        return

    try:
        output = to_markdown(path)
        output = output.replace("\n", "")

        if output == "":
            logger.error("no text detected, either the pdf cannot be processed or the ocr engine is not installed.")

    except Exception as error:
        logger.error(f"error occurred while extracting text: {str(error)}")


@flow(name="media-manager")
def process_document(doc_info: dict):
    doc = download_document(doc_info.get("url"))
    text = extract_text(doc)


if __name__ == "__main__":
    process_document.serve(
        name="media-manager"
    )
