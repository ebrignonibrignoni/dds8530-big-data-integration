"""
INTEGRATION: schema matching and entity resolution, each evaluated against the
Alaska curated ground truth.

Schema matching (which source attribute means which target attribute?)
  M1  name similarity: compare normalized attribute names with target names
  M2  learned dictionary: learn name->target mappings from half of the websites'
      labels and apply them to the other half (two-fold cross-validation by website)

Entity resolution (which listings describe the same real-world monitor?)
  blocking on model codes extracted from page titles, brand check, union-find clustering,
  pairwise precision / recall / F1 on the labeled records
"""
import difflib
import itertools
import re
from collections import defaultdict

import pandas as pd

from bdi.clean import norm_attr
from bdi.config import SEED, STAGE, get_logger
from bdi.ingest import ground_truth

log = get_logger("integrate")


# ---------------------------------------------------------------- schema matching
def _prf(tp, fp, fn):
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    f = 2 * p * r / (p + r) if p + r else 0.0
    return {"precision": round(p, 3), "recall": round(r, 3), "f1": round(f, 3), "tp": tp, "fp": fp, "fn": fn}


def _tokens(s):
    return set(re.findall(r"[a-z0-9]+", s.lower()))


def name_similarity(attr: str, target: str) -> float:
    t = target.replace("_", " ")
    jac = len(_tokens(attr) & _tokens(t)) / max(1, len(_tokens(attr) | _tokens(t)))
    return max(jac, difflib.SequenceMatcher(None, attr, t).ratio())


def schema_matching() -> dict:
    _, sm = ground_truth()
    sm = sm.drop_duplicates()
    sm[["source", "attribute"]] = sm["source_attribute_id"].str.split("//", n=1, expand=True)
    sm["attribute"] = sm["attribute"].map(norm_attr)
    targets = sorted(sm["target_attribute_name"].unique())
    truth = set(zip(sm.source, sm.attribute, sm.target_attribute_name))
    labeled = sm[["source", "attribute"]].drop_duplicates()

    # M1: best-scoring target above a threshold
    pred1 = set()
    for s, a in labeled.itertuples(index=False):
        best = max(targets, key=lambda t: name_similarity(a, t))
        if name_similarity(a, best) >= 0.6:
            pred1.add((s, a, best))
    m1 = _prf(len(pred1 & truth), len(pred1 - truth), len(truth - pred1))

    # M2: dictionary learned from labeled websites, applied to unseen websites
    sites = sorted(sm.source.unique())
    rng = pd.Series(sites).sample(frac=1, random_state=SEED).tolist()
    folds = [set(rng[::2]), set(rng[1::2])]
    pred2 = set()
    for i in (0, 1):
        train = sm[sm.source.isin(folds[i])]
        dictionary = defaultdict(set)
        for a, t in zip(train.attribute, train.target_attribute_name):
            dictionary[a].add(t)
        for s, a in labeled[labeled.source.isin(folds[1 - i])].itertuples(index=False):
            for t in dictionary.get(a, ()):
                pred2.add((s, a, t))
    m2 = _prf(len(pred2 & truth), len(pred2 - truth), len(truth - pred2))
    res = {"labeled_source_attributes": len(labeled), "target_attributes": len(targets),
           "correspondences": len(truth), "name_similarity": m1, "learned_dictionary": m2}
    log.info("Schema matching: %s", res)
    return res


# ---------------------------------------------------------------- entity resolution
UNIT = re.compile(r"^\d+(hz|ms|cm|mm|in|inch|w|v|kg|lbs|gb|mb|nits|k|p|cdm2|cd|dpi|bit)$|^\d{3,4}x\d{3,4}$|^\d+$")


def model_codes(title: str) -> set:
    """Candidate model identifiers: tokens mixing letters and digits, e.g. VE247H, P2419H, 24MK430H."""
    if not isinstance(title, str):
        return set()
    out = set()
    for tok in re.findall(r"[A-Za-z0-9][A-Za-z0-9\-/]{3,19}", title):
        t = tok.lower().replace("-", "").replace("/", "")
        if len(t) >= 4 and re.search(r"[a-z]", t) and re.search(r"\d", t) and not UNIT.match(t):
            out.add(t)
    return out


def entity_resolution() -> dict:
    er, _ = ground_truth()
    dup_rows = int(er.duplicated().sum())
    er = er.drop_duplicates()
    conflict = er.groupby("spec_id")["entity_id"].nunique()
    conflicting_specs = int((conflict > 1).sum())
    feats = pd.read_parquet(STAGE / "products.parquet")
    labeled = feats[feats.spec_id.isin(er.spec_id)].copy()

    # blocking: index every labeled listing by each model code in its title
    # boilerplate filter: a code repeated across many titles of one website (shop names,
    # category labels) is not a model identifier
    feats["codes"] = feats["page_title"].map(model_codes)
    boiler = set()
    for src, g in feats.groupby("source"):
        freq = pd.Series([c for cs in g.codes for c in cs]).value_counts()
        boiler |= set(freq[(freq > 5) & (freq > 0.02 * len(g))].index)
    labeled["codes"] = labeled["page_title"].map(lambda t: model_codes(t) - boiler)
    block = defaultdict(set)
    for sid, codes in zip(labeled.spec_id, labeled.codes):
        for c in codes:
            block[c].add(sid)
    brand = {s: (b if isinstance(b, str) else None) for s, b in zip(labeled.spec_id, labeled.brand)}
    parent = {s: s for s in labeled.spec_id}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    candidate_pairs = 0
    for code, members in block.items():
        if len(members) > 60:           # a code shared by many listings is not a model identifier
            continue
        for a, b in itertools.combinations(sorted(members), 2):
            candidate_pairs += 1
            if brand[a] and brand[b] and brand[a] != brand[b]:
                continue
            parent[find(a)] = find(b)

    pred_clusters = defaultdict(set)
    for s in parent:
        pred_clusters[find(s)].add(s)
    pred_pairs = {tuple(sorted(p)) for c in pred_clusters.values() for p in itertools.combinations(c, 2)}
    true_pairs = {tuple(sorted(p)) for _, g in er.groupby("entity_id") for p in itertools.combinations(g.spec_id, 2)}
    m = _prf(len(pred_pairs & true_pairs), len(pred_pairs - true_pairs), len(true_pairs - pred_pairs))
    n = len(parent)
    res_extra = {"boilerplate_codes_removed": len(boiler)}
    res = {"gt_duplicate_rows_removed": dup_rows, "gt_specs_in_two_entities": conflicting_specs,
           "labeled_listings": n, "true_entities": int(er.entity_id.nunique()),
           "predicted_clusters": len(pred_clusters), "blocks": len(block),
           "candidate_pairs": candidate_pairs, "all_pairs_without_blocking": n * (n - 1) // 2,
           "pairwise": m, **res_extra}
    sizes = pd.Series([len(c) for c in pred_clusters.values()], name="size")
    sizes.to_frame().to_parquet(STAGE / "er_cluster_sizes.parquet", index=False)
    log.info("Entity resolution: %s", res)
    return res
