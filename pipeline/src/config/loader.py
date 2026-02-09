import tomllib
from pathlib import Path

from prefect.cache_policies import (
    DEFAULT,
    FLOW_PARAMETERS,
    INPUTS,
    NO_CACHE,
    NONE,
    RUN_ID,
    STABLE_TRANSFORMS,
    TASK_SOURCE,
)
from pydantic import BaseModel


class FlowConfig(BaseModel):
    MAX_RUNNERS: int
    TASKS_RETURN_EXCEPTION: bool
    DATE_LOOKBACK: int


class AllEndpoints(BaseModel):
    FETCH_MAX_RETRIES: int
    FETCH_RETRY_DELAY: int


class TSEConfig(BaseModel):
    BASE_URL: str
    OUTPUT_EXTRACT_DIR: str
    TASK_RETRIES: int
    TASK_RETRY_DELAY: int
    TASK_TIMEOUT: int
    CACHE_POLICY: str
    CACHE_EXPIRATION: int


class CamaraConfig(BaseModel):
    REST_BASE_URL: str
    PORTAL_BASE_URL: str
    OUTPUT_EXTRACT_DIR: str
    TASK_RETRIES: int
    TASK_RETRY_DELAY: int
    TASK_TIMEOUT: int
    FETCH_LIMIT: int


class SenadoConfig(BaseModel):
    REST_BASE_URL: str
    OUTPUT_EXTRACT_DIR: str
    TASK_RETRIES: int
    TASK_RETRY_DELAY: int
    TASK_TIMEOUT: int
    FETCH_LIMIT: int


class AppConfig(BaseModel):
    FLOW: FlowConfig
    ALLENDPOINTS: AllEndpoints
    TSE: TSEConfig
    CAMARA: CamaraConfig
    SENADO: SenadoConfig


CONFIG_PATH = "appsettings.toml"


def load_config(path: str | Path = CONFIG_PATH) -> AppConfig:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"O arquivo configuração 'appsettings.toml' não foi encontrado em {path}"
        )
    with path.open("rb") as f:
        raw = tomllib.load(f)

    return AppConfig(**raw)


CACHE_POLICY_MAP = {
    "INPUTS": INPUTS,
    "NONE": NONE,
    "DEFAULT": DEFAULT,
    "TASK_SOURCE": TASK_SOURCE,
    "FLOW_PARAMETERS": FLOW_PARAMETERS,
    "RUN_ID": RUN_ID,
    "STABLE_TRANSFORMS": STABLE_TRANSFORMS,
    "NO_CACHE": NO_CACHE,
}
