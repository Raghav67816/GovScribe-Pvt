from pydantic import BaseModel
from sqlmodel import SQLModel, Field

"""
RepoUrl

url: website url
"""
class RepoUrl(BaseModel):
    url: str

"""
Repo

url: website url (unique)
title: website title
"""
class Repo(SQLModel, table=True):

    __tablename__ = "repos"

    id: int | None = Field(default=None, primary_key=True)
    url: str = Field(unique=True)
    title: str

"""
RepoOutput

url: website url
title: page title
status: inserted, already exists, error
"""
class RepoOutput(BaseModel):
    url: str
    title: str
    status: str

"""
DownloadedRepo

url: website url
filepath: location of the webpage downloaded
"""
class DownloadedRepo(BaseModel):
    url: str
    filepath: str