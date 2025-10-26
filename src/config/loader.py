from pathlib import Path
import tomllib
from pydantic import BaseModel
from prefect.cache_policies import (
    DEFAULT,
    TASK_SOURCE,
    FLOW_PARAMETERS,
    INPUTS,
    NO_CACHE,
    NONE,
    RUN_ID,
    STABLE_TRANSFORMS                     
)

class FlowConfig(BaseModel):
    MAX_RUNNERS: int

class TSEConfig(BaseModel):
    BASE_URL: str
    RETRIES: int
    RETRY_DELAY: int
    TIMEOUT: int
    CACHE_POLICY: str
    CACHE_EXPIRATION: int

class CongressoConfig(BaseModel):
    REST_BASE_URL: str
    RETRIES: int
    RETRY_DELAY: int
    TIMEOUT: int
    CONCURRENCY: int

class AppConfig(BaseModel):
    FLOW: FlowConfig
    TSE: TSEConfig
    CAMARA: CongressoConfig

CONFIG_PATH = "appsettings.toml"

def load_config(path: str | Path = CONFIG_PATH) -> AppConfig:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"O arquivo configuração 'appsettings.toml' não foi encontrado em {path}")
    with path.open("rb") as f:
        raw = tomllib.load(f)

    return AppConfig(**raw)

CACHE_POLICY_MAP = {
    "INPUTS": INPUTS,
    "NONE": NONE,
    "NO_CACHE": NONE,
    "DEFAULT": DEFAULT,
    "TASK_SOURCE": TASK_SOURCE,
    "FLOW_PARAMETERS": FLOW_PARAMETERS,
    "RUN_ID": RUN_ID,
    "STABLE_TRANSFORMS": STABLE_TRANSFORMS,
    "NO_CACHE": NO_CACHE
}