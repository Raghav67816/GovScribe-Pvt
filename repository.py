from app import get_db_session

from os.path import join
from os import getcwd, makedirs

from prefect import task, flow
from prefect.events import emit_event
from prefect.logging import get_run_logger

from bs4 import BeautifulSoup
from curl_cffi import requests
from sqlmodel import Session, select

from models.repo_models import(
    Repo, 
    RepoUrl, 
    RepoOutput,
    DownloadedRepo 
)

@task(name="add-repo")
def add_repo(inp: RepoUrl) -> dict:
    logger = get_run_logger()
    statement = select(Repo).where(Repo.url == inp.url)

    with get_db_session() as session:
        exst = session.exec(statement).first()
        if exst:
            logger.error("repo already exists")
            return RepoOutput(
                url=str(exst.url), title="Example", status="repo already exists"
            ).model_dump()

        repo = Repo(url=inp.url, title="Sample")

        session.add(repo)
        session.commit()
        session.refresh(repo)

        return RepoOutput(
            url=str(repo.url), title="Example", status="inserted"
        ).model_dump()


@task(name="download-repo", retries=3, retry_delay_seconds=10)
def download_repo(repo_info: dict) -> dict:

    logger = get_run_logger()

    try:        
        url = repo_info.get("url")
        status = repo_info.get("status")

        if status == "repo already exists":
            logger.info("Skipping download: Repo already exists.")
            return DownloadedRepo(url=url, filepath="", status="skipped").model_dump()

        logger.info(f"downloading: {url}")

        output_folder = f"{getcwd()}/webpages"
        makedirs(output_folder, exist_ok=True)

        response = requests.get(
            url=url, verify=False, impersonate="chrome", timeout=30
        )

        if response.status_code == 200:
            filename = (
                url.split("URL=")[-1].strip().replace("%", "_")
                if "URL=" in url
                else "index"
            )
            filepath = join(output_folder, f"{filename}.html")

            with open(filepath, "w+") as html_file:
                html_file.write(response.text)

            logger.info(f"file downloaded at: {filepath}")

            return DownloadedRepo(url=url, filepath=filepath, status="downloaded").model_dump()

        else:
            raise Exception(f"Bad status code: {response.status_code}")

    except Exception as e:
        logger.error(f"Unexpected failure in download: {str(e)}")
        return DownloadedRepo(url="", filepath="", status="error").model_dump()


@task(name="process-repo")
def process_repo(repo_info: dict):

    logger = get_run_logger()

    if not repo_info or repo_info.get("status") in ["skipped", "error"] or not repo_info.get("filepath"):
        logger.info("Skipping process_repo task execution.")
        return

    logger.info(f"reading: {repo_info.get('filepath')}")

    with open(repo_info.get("filepath"), "r") as html:
        soup = BeautifulSoup(html.read(), "html.parser")

        if soup.title and soup.title.string:
            title = soup.title.string.strip()
        else:
            title = "untitled"

    statement = select(Repo).where(Repo.url == repo_info.get("url"))

    with get_db_session() as session:
        repo = session.exec(statement).first()
        if repo:
            repo.title = title
            logger.info(f"updated title to {repo.title}")
            session.add(repo)
            session.commit()
            session.refresh(repo)
        else:
            logger.warning("no existing repo found")

    num_docs = 0
    all_a = soup.find_all("a")
    for a in all_a:
        if a.get("href") and a["href"].endswith(".pdf"):
            num_docs += 1

    logger.info(f"discovered {num_docs} documents")
    return all_a


@flow(name="reop-workflow")
def repo_workflow(inp: RepoUrl):
    add_output = add_repo(inp)
    downloaded_out = download_repo(add_output)
    pdf_list = process_repo(downloaded_out)

    for link in pdf_list:
        emit_event(
            event="media.discovered",
            resource={"prefect.resource.id": f"url"},
            payload={"url": str(link)}
        )

if __name__ == "__main__":
    repo_workflow.serve(
        name="repo-worker"
    )
