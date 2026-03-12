from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
import inspect


def logs_config(log_level: int = logging.INFO):
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Detect script name to save file correctly
    frame = inspect.stack()[1]
    caller_module = inspect.getmodule(frame[0])

    if caller_module and hasattr(caller_module, "__file__"):
        script_name = Path(caller_module.__file__).stem
    else:
        script_name = "app"

    # Log filename
    log_file = log_dir / f"{script_name}.log"

    # Format
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)

    # Specific logger
    logger = logging.getLogger(script_name)
    logger.setLevel(log_level)
    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.propagate = False

    logger.info("=" * 60)
    logger.info(f"Script: {script_name}")
    logger.info(f"Log file: {log_file.absolute()}")
    logger.info("=" * 60)

    return logger