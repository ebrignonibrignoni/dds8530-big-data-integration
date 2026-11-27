"""
INGEST: pull records from every source system into one unified long table
(spec_id, source, origin, attribute, value) plus a title table.
  - MongoDB (PyMongo): nested JSON documents are flattened
  - SQL (SQLAlchemy): entity-attribute-value rows are read with one join
  - CSV (pandas): ground truth files
"""
import json

import pandas as pd
from sqlalchemy import create_engine, text

from bdi.config import GT_DIR, SQL_URL, STAGE, get_logger
from bdi.sources import mongo_collection

log = get_logger("ingest")


def from_mongo() -> tuple[pd.DataFrame, pd.DataFrame]:
    rows, titles = [], []
    for doc in mongo_collection().find({}, batch_size=2000):
        sid, src = doc["_id"], doc["source"]
        titles.append({"spec_id": sid, "source": src, "origin": "mongodb", "page_title": doc.get("<page title>")})
        for k, v in doc.items():
            if k in ("_id", "source", "<page title>"):
                continue
            rows.append({"spec_id": sid, "source": src, "origin": "mongodb", "attribute": k,
                         "value": " | ".join(map(str, v)) if isinstance(v, list) else v})
    log.info("MongoDB: %d documents, %d attribute values", len(titles), len(rows))
    return pd.DataFrame(rows), pd.DataFrame(titles)


def from_sql() -> tuple[pd.DataFrame, pd.DataFrame]:
    engine = create_engine(SQL_URL)
    with engine.connect() as conn:
        titles = pd.read_sql(text("SELECT spec_id, source, page_title FROM products"), conn)
        rows = pd.read_sql(text("""SELECT a.spec_id, p.source, a.attribute, a.value
                                   FROM product_attributes a JOIN products p ON p.spec_id = a.spec_id"""), conn)

    def unlist(v):
        if isinstance(v, str) and v.startswith("["):
            try:
                return " | ".join(map(str, json.loads(v)))
            except ValueError:
                return v
        return v
    rows["value"] = rows["value"].map(unlist)
    rows["origin"] = titles["origin"] = "sql"
    log.info("SQL: %d products, %d attribute values", len(titles), len(rows))
    return rows, titles


def ground_truth() -> tuple[pd.DataFrame, pd.DataFrame]:
    er = pd.read_csv(GT_DIR / "monitor_entity_resolution_gt.csv")
    sm = pd.read_csv(GT_DIR / "monitor_schema_matching_gt.csv")
    return er, sm


def ingest() -> dict:
    m_rows, m_titles = from_mongo()
    s_rows, s_titles = from_sql()
    rows = pd.concat([m_rows, s_rows], ignore_index=True)
    titles = pd.concat([m_titles, s_titles], ignore_index=True)
    rows["value"] = rows["value"].astype("string")
    rows.to_parquet(STAGE / "attributes_raw.parquet", index=False)
    titles.to_parquet(STAGE / "titles.parquet", index=False)
    er, sm = ground_truth()
    stats = {"records": len(titles), "sources": titles.source.nunique(), "attribute_values": len(rows),
             "distinct_attribute_names": rows.attribute.nunique(),
             "mongodb_records": len(m_titles), "sql_records": len(s_titles),
             "er_gt_rows": len(er), "sm_gt_rows": len(sm)}
    log.info("Unified: %s", stats)
    return stats
