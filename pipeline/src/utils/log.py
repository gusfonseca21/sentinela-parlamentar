from typing import Any

from prefect import get_run_logger
from prefect.exceptions import MissingContextError


def get_prefect_logger_or_none() -> Any | None:
    try:
        return get_run_logger()
    except MissingContextError:
        return None
