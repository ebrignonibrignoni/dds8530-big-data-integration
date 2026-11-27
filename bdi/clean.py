"""
CLEAN AND TRANSFORM with pandas (baseline) and Dask (parallel).
  - normalizes attribute names and values (case, whitespace, line breaks)
  - removes empty values and exact duplicate rows
  - resolves inconsistent units and labels into typed features:
    brand, screen size in inches (from inch, cm, or title text), resolution, refresh rate
"""
import re

import dask.dataframe as dd
import numpy as np
import pandas as pd

from bdi.config import STAGE, get_logger

log = get_logger("clean")

BRANDS = ["acer", "aoc", "asus", "benq", "dell", "eizo", "fujitsu", "hp", "hewlett packard", "iiyama", "lenovo",
          "lg", "nec", "philips", "samsung", "sharp", "sony", "viewsonic", "toshiba", "planar", "hannspree",
          "elo", "msi", "apple", "lacie", "ctl", "gigabyte", "medion", "packard bell", "compaq", "neovo"]
BRAND_ALIASES = {"hewlett packard": "hp", "hewlett-packard": "hp", "lg electronics": "lg",
                 "samsung electronics": "samsung", "nec display": "nec", "packard bell": "packard bell"}
SIZE_ATTRS = re.compile(r"(?:screen|display|panel|diagonal|viewable).*(?:size|diag)|^size$|^diagonal")
INCH = re.compile(r"(\d{2}(?:[.,]\d{1,2})?)\s*(?:\"|''|”|in\b|inch|zoll|pollici|pouces)", re.I)
CM = re.compile(r"(\d{2,3}(?:[.,]\d)?)\s*cm", re.I)
RES = re.compile(r"(\d{3,4})\s*[x×]\s*(\d{3,4})")
HZ = re.compile(r"(\d{2,3})\s*hz", re.I)


def norm_attr(a: str) -> str:
    a = re.sub(r"[\s_]+", " ", str(a).lower()).strip(" :.-")
    return a


def norm_value(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    v = re.sub(r"\s+", " ", str(v)).strip()
    return v or None


def parse_size(text):
    if not isinstance(text, str):
        return np.nan
    m = INCH.search(text)
    if m:
        x = float(m.group(1).replace(",", "."))
        return x if 10 <= x <= 100 else np.nan
    m = CM.search(text)
    if m:
        x = float(m.group(1).replace(",", ".")) / 2.54
        return round(x, 1) if 10 <= x <= 100 else np.nan
    return np.nan


def find_brand(text):
    if not isinstance(text, str):
        return None
    t = text.lower()
    for alias, b in BRAND_ALIASES.items():
        if alias in t:
            return b
    for b in BRANDS:
        if re.search(rf"\b{re.escape(b)}\b", t):
            return b
    return None


def clean_rows_pandas(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    rep = {"rows_in": len(raw)}
    df = raw.copy()
    df["attribute"] = df["attribute"].map(norm_attr)
    df["value"] = df["value"].map(norm_value)
    df = df.dropna(subset=["value"])
    rep["after_empty_removed"] = len(df)
    df = df.drop_duplicates(subset=["spec_id", "attribute", "value"])
    rep["after_duplicates_removed"] = len(df)
    return df, rep


def clean_rows_dask(raw: pd.DataFrame, partitions: int = 8) -> pd.DataFrame:
    ddf = dd.from_pandas(raw, npartitions=partitions)
    ddf["attribute"] = ddf["attribute"].map(norm_attr, meta=("attribute", "object"))
    ddf["value"] = ddf["value"].map(norm_value, meta=("value", "object"))
    ddf = ddf.dropna(subset=["value"])
    return ddf.drop_duplicates(subset=["spec_id", "attribute", "value"]).compute()


def build_features(rows: pd.DataFrame, titles: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Pivot the heterogeneous attributes into one typed row per product."""
    t = titles.set_index("spec_id")
    size_rows = rows[rows["attribute"].str.contains(SIZE_ATTRS)]
    size_attr = size_rows.assign(size=size_rows["value"].map(parse_size)).dropna(subset=["size"]) \
        .groupby("spec_id")["size"].first()
    brand_rows = rows[rows["attribute"].isin(["brand", "manufacturer", "brand name", "marca", "marque", "hersteller"])]
    brand_attr = brand_rows.assign(b=brand_rows["value"].map(find_brand)).dropna(subset=["b"]) \
        .groupby("spec_id")["b"].first()
    joined = rows.groupby("spec_id")["value"].apply(lambda s: " ".join(s))
    feats = pd.DataFrame(index=t.index)
    feats["source"], feats["origin"], feats["page_title"] = t["source"], t["origin"], t["page_title"]
    feats["n_attributes"] = rows.groupby("spec_id").size().reindex(t.index).fillna(0).astype(int)
    feats["brand"] = brand_attr.reindex(t.index)
    title_brand = t["page_title"].map(find_brand)
    feats["brand_from_title"] = feats["brand"].isna() & title_brand.notna()
    feats["brand"] = feats["brand"].fillna(title_brand)
    feats["screen_in"] = size_attr.reindex(t.index)
    title_size = t["page_title"].map(parse_size)
    feats["size_from_title"] = feats["screen_in"].isna() & title_size.notna()
    feats["screen_in"] = feats["screen_in"].fillna(title_size)
    blob = (t["page_title"].fillna("") + " " + joined.reindex(t.index).fillna(""))
    res = blob.str.extract(RES).astype(float)
    feats["res_w"], feats["res_h"] = res[0], res[1]
    feats["refresh_hz"] = blob.str.extract(HZ)[0].astype(float).where(lambda x: x.between(24, 360))
    norm_title = t["page_title"].fillna("").str.lower().str.replace(r"[^a-z0-9]+", " ", regex=True).str.strip()
    feats["duplicate_listing"] = norm_title.groupby(t["source"]).transform(lambda s: s.duplicated(keep="first"))
    feats = feats.reset_index()
    rep = {"products": len(feats),
           "brand_missing_pct": float(round(feats["brand"].isna().mean() * 100, 1)),
           "brand_recovered_from_title": int(feats["brand_from_title"].sum()),
           "screen_missing_pct": float(round(feats["screen_in"].isna().mean() * 100, 1)),
           "screen_recovered_from_title": int(feats["size_from_title"].sum()),
           "resolution_missing_pct": float(round(feats["res_w"].isna().mean() * 100, 1)),
           "distinct_brands": int(feats["brand"].nunique()),
           "duplicate_listings_flagged": int(feats["duplicate_listing"].sum())}
    return feats, rep


def clean() -> dict:
    raw = pd.read_parquet(STAGE / "attributes_raw.parquet")
    titles = pd.read_parquet(STAGE / "titles.parquet")
    raw_names = raw["attribute"].nunique()
    rows, rep = clean_rows_pandas(raw)
    rows_dask = clean_rows_dask(raw)
    assert len(rows_dask) == len(rows), "Dask and pandas cleaning disagree"
    rep["distinct_attribute_names_raw"] = int(raw_names)
    rep["distinct_attribute_names_normalized"] = int(rows["attribute"].nunique())
    feats, frep = build_features(rows, titles)
    rep.update(frep)
    rows.to_parquet(STAGE / "attributes_clean.parquet", index=False)
    feats.to_parquet(STAGE / "products.parquet", index=False)
    log.info("Cleaning report: %s", rep)
    return rep
