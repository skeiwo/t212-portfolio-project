import logging
from prefect.exceptions import MissingContextError
from prefect.logging import get_run_logger

def _get_logger():
    try:
        logger = get_run_logger()
    except MissingContextError:
        logger = logging.getLogger(__name__)
    return logger