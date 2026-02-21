import os
from dataclasses import dataclass

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@dataclass
class DBConfig:
    url: str


def load_db_config() -> DBConfig:
    url = os.environ.get("DATABASE_URL", "sqlite:///./protein_dw.sqlite")
    return DBConfig(url=url)


def build_engine(config: DBConfig):
    kwargs = {}
    if config.url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(config.url, **kwargs)


def build_session_factory(engine):
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)
