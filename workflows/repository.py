import logging
from os import getcwd, makedirs
from os.path import join

from bs4 import BeautifulSoup
from client import hatchet_client, psql_engine
from curl_cffi import requests
from hatchet_sdk import Context
from models.repo_models import DownloadedRepo, Repo, RepoUrl, RepoOutput
from sqlmodel import Session, select

WORKFLOW_NAME = "repo-workflow"

repo_workflow = hatchet_client.workflow(name=WORKFLOW_NAME)
logger = logging.getLogger(name=WORKFLOW_NAME)

@repo_workflow.task(name="add-repo")
def add_repo(inp: RepoUrl, ctx: Context) -> dict:
    statement = select(Repo).where(Repo.url == inp.url)

    with Session(psql_engine) as session:
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


@repo_workflow.task(name="download-repo", parents=[add_repo])
def download_repo(inp: RepoUrl, ctx: Context) -> dict:
    try:
        parent_output = ctx.task_output(add_repo)
        
        url = parent_output.get("url")
        status = parent_output.get("status")

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


@repo_workflow.task(name="process-repo", parents=[download_repo])
def process_repo(inp: dict, ctx: Context):
    print("ok", flush=True)
    parent_output = ctx.task_output(download_repo)

    if not parent_output or parent_output.get("status") in ["skipped", "error"] or not parent_output.get("filepath"):
        logger.info("Skipping process_repo task execution.")
        return

    logger.info(f"reading: {parent_output.get('filepath')}")

    with open(parent_output.get("filepath"), "r") as html:
        soup = BeautifulSoup(html.read(), "html.parser")

        if soup.title and soup.title.string:
            title = soup.title.string.strip()
        else:
            title = "untitled"

    statement = select(Repo).where(Repo.url == parent_output.get("url"))

    with Session(psql_engine) as session:
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

            hatchet_client.event.push(
                event_key="media:discovered",
                payload={"url": a['href']}
            )

    logger.info(f"discovered {len(num_docs)} documents")


worker = hatchet_client.worker("repo-worker", workflows=[repo_workflow])
worker.start()
