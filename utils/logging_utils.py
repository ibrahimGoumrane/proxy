import logging
from datetime import datetime, timezone
from pathlib import Path


def setup_logger(log_dir: Path) -> logging.Logger:
	log_dir.mkdir(parents=True, exist_ok=True)
	run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
	log_file = log_dir / f"batch_{run_stamp}.log"

	logger = logging.getLogger("match_sync")
	logger.setLevel(logging.INFO)
	logger.handlers.clear()
	logger.propagate = False

	handler = logging.FileHandler(log_file, encoding="utf-8")
	handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
	logger.addHandler(handler)

	return logger
