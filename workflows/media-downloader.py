from logging import getLogger
from client import hatchet_client

from hatchet_sdk import Context
from models.repo_models import RepoUrl

WORKFLOW_NAME = "media-download"

downloader_workflow = hatchet_client.workflow(
    name=WORKFLOW_NAME,
    on_events=['media:discovered']
)

logger = getLogger(name=WORKFLOW_NAME)

@downloader_workflow.task(
    name="download-media"
)
def download_media(out: RepoUrl, ctx: Context):
    print(out.url)

if __name__ == "__main__":
    worker = hatchet_client.worker("media-worker", workflows=[downloader_workflow])
    worker.start()
