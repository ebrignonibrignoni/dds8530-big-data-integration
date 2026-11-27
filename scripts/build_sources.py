"""
Downloads the Alaska monitor vertical and distributes it across heterogeneous
source systems, mimicking a real integration landscape:
  - 13 websites  -> MongoDB collection `specs` (raw JSON documents, NoSQL)
  - 13 websites  -> relational tables `products` and `product_attributes` (SQL, EAV layout)
  - ground truths stay as CSV files
"""
import json
import random
import sys
import tarfile
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bdi.config import ALASKA_URL, NOSQL_SHARE, RAW, SEED, SPECS_DIR, SQL_URL, get_logger  # noqa: E402
from bdi.sources import mongo_collection  # noqa: E402

log = get_logger("build_sources")


def download():
    if SPECS_DIR.exists():
        return
    archive = RAW / "monitor.tar.gz"
    if not archive.exists():
        log.info("Downloading Alaska monitor vertical from %s", ALASKA_URL)
        archive.write_bytes(requests.get(ALASKA_URL, timeout=120).content)
    with tarfile.open(archive) as tar:
        tar.extractall(RAW, filter="data")


def main():
    download()
    sources = sorted(p.name for p in SPECS_DIR.iterdir() if p.is_dir())
    random.Random(SEED).shuffle(sources)
    k = int(len(sources) * NOSQL_SHARE)
    nosql, sql = sorted(sources[:k]), sorted(sources[k:])

    docs, products, attrs = [], [], []
    for src in sources:
        for f in sorted((SPECS_DIR / src).glob("*.json")):
            spec = json.loads(f.read_text(encoding="utf-8"))
            spec_id = f"{src}//{f.stem}"
            if src in nosql:
                docs.append({"_id": spec_id, "source": src, **spec})
            else:
                products.append({"spec_id": spec_id, "source": src, "page_title": spec.get("<page title>")})
                attrs += [{"spec_id": spec_id, "attribute": a, "value": json.dumps(v) if isinstance(v, list) else v}
                          for a, v in spec.items() if a != "<page title>"]

    coll = mongo_collection()
    coll.delete_many({})
    coll.insert_many(docs)
    engine = create_engine(SQL_URL)
    pd.DataFrame(products).to_sql("products", engine, if_exists="replace", index=False)
    pd.DataFrame(attrs).to_sql("product_attributes", engine, if_exists="replace", index=False, chunksize=20000)
    log.info("MongoDB: %d documents from %d sites | SQL: %d products, %d attribute rows from %d sites",
             len(docs), len(nosql), len(products), len(attrs), len(sql))


if __name__ == "__main__":
    main()
