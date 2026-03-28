import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from utils import mean, normalize_header, parse_float, parse_int, pick_column


def test_parse_float_valid():
    assert parse_float("3.14") == 3.14
    assert parse_float("0") == 0.0
    assert parse_float("-1.5") == -1.5
    assert parse_float("  42  ") == 42.0


def test_parse_float_none_and_empty():
    assert parse_float(None) is None
    assert parse_float("") is None
    assert parse_float("  ") is None
    assert parse_float("NA") is None
    assert parse_float("na") is None
    assert parse_float("null") is None


def test_parse_float_invalid():
    assert parse_float("abc") is None
    assert parse_float("N/A") is None


def test_parse_int_valid():
    assert parse_int("42") == 42
    assert parse_int("3.9") == 3
    assert parse_int("  100  ") == 100


def test_parse_int_none_and_empty():
    assert parse_int(None) is None
    assert parse_int("") is None
    assert parse_int("NA") is None
    assert parse_int("null") is None


def test_parse_int_invalid():
    assert parse_int("abc") is None


def test_mean_values():
    assert mean([1.0, 2.0, 3.0]) == 2.0
    assert mean([10.0, None, 20.0]) == 15.0
    assert mean([None, None]) is None
    assert mean([]) is None
    assert mean([5.0]) == 5.0


def test_normalize_header():
    assert normalize_header("State Name") == "statename"
    assert normalize_header("EHR_Adoption_%") == "ehradoption"
    assert normalize_header("  rural pct  ") == "ruralpct"


def test_pick_column_exact():
    headers = ["State", "EHR_Adoption", "HIE_Exchange"]
    assert pick_column(headers, ["EHR_Adoption"]) == "EHR_Adoption"


def test_pick_column_normalized():
    headers = ["State Name", "ehr_adoption_pct"]
    assert pick_column(headers, ["EHR Adoption Pct"]) == "ehr_adoption_pct"


def test_pick_column_partial():
    headers = ["state_code", "ehr_adoption_pct_2024"]
    result = pick_column(headers, ["ehr_adoption_pct"])
    assert result == "ehr_adoption_pct_2024"


def test_pick_column_not_found():
    headers = ["state", "population"]
    assert pick_column(headers, ["enrollment"]) is None
