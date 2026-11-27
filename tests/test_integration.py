"""Unit tests for the normalization and integration logic (run in GitHub Actions)."""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bdi.clean import clean_rows_dask, clean_rows_pandas, find_brand, norm_attr, parse_size  # noqa: E402
from bdi.integrate import model_codes, name_similarity  # noqa: E402


@pytest.mark.parametrize("text,expected", [('17"', 17.0), ("24 inch", 24.0), ("27 Zoll", 27.0),
                                           ("68.6 cm", 27.0), ("Dell U2913WM 29in", 29.0), ("no size", None)])
def test_parse_size_handles_units(text, expected):
    got = parse_size(text)
    assert (pd.isna(got) and expected is None) or got == pytest.approx(expected, abs=0.1)


def test_brand_aliases_and_case():
    assert find_brand("Hewlett-Packard Compaq LE1711") == "hp"
    assert find_brand("SAMSUNG S24D590PL") == "samsung"
    assert find_brand("Generic panel") is None


def test_attribute_names_normalized():
    assert norm_attr("  Screen_Size: ") == "screen size"


def test_pandas_and_dask_cleaning_agree():
    raw = pd.DataFrame({"spec_id": ["a", "a", "a", "b"], "source": ["s"] * 4,
                        "attribute": ["Brand", "brand ", "Size", "Brand"],
                        "value": ["Dell", "Dell", "  ", "HP\n"]})
    p, rep = clean_rows_pandas(raw)
    d = clean_rows_dask(raw, partitions=2)
    assert len(p) == len(d) == 2          # duplicate and empty value removed
    assert rep["after_empty_removed"] == 3


def test_model_codes_skip_units():
    codes = model_codes("ASUS VE247H 24in 1920x1080 60Hz 2ms Monitor")
    assert "ve247h" in codes and "60hz" not in codes and "1920x1080" not in codes


def test_name_similarity_ranks_matching_target_higher():
    assert name_similarity("screen size", "screen_size_diagonal") > name_similarity("screen size", "color")
