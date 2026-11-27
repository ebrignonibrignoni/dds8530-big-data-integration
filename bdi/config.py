"""Central configuration. Every external system is set through an environment
variable so the same code runs locally and in GitHub Actions."""
import logging
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW = DATA / "raw"                       # downloaded Alaska archive
SPECS_DIR = RAW / "monitor" / "monitor_specs"
GT_DIR = RAW / "monitor" / "monitor_ground_truths"
STAGE = DATA / "stage"                   # unified and cleaned outputs (Parquet)
RESULTS = ROOT / "results"
FIGURES = ROOT / "figures"
for d in (RAW, STAGE, RESULTS, FIGURES):
    d.mkdir(parents=True, exist_ok=True)

ALASKA_URL = "https://github.com/merialdo/research.alaska/raw/main/monitor.tar.gz"

# Source systems
SQL_URL = os.getenv("SQL_URL", f"sqlite:///{DATA / 'catalog.db'}")      # relational (PostgreSQL in CI)
MONGO_URI = os.getenv("MONGO_URI", "")                                   # empty -> in-memory mongomock
MONGO_DB = "alaska"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "")                        # empty -> streaming step skipped
KAFKA_TOPIC = "monitor_specs"

# Half of the 26 websites are served from MongoDB (JSON documents), half from SQL.
NOSQL_SHARE = 0.5
SEED = 8530


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                        handlers=[logging.StreamHandler(), logging.FileHandler(RESULTS / "pipeline.log")])
    return logging.getLogger(name)
