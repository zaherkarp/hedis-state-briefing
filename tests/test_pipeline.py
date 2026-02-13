import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"

sys.path.append(str(SCRIPTS))

import process  # noqa: E402
import build  # noqa: E402


def copy_samples(tmp_path: Path) -> Path:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    samples = ROOT / "data" / "samples" / "raw"
    for sample in samples.glob("*.csv"):
        shutil.copy2(sample, raw_dir / sample.name)
    return raw_dir


def run_full_pipeline(tmp_path, monkeypatch):
    """Helper: run process + build and return (out_dir, web_dir)."""
    raw_dir = copy_samples(tmp_path)
    processed = tmp_path / "processed"
    processed.mkdir()

    process.process_onc(raw_dir, processed)
    process.process_ruca(raw_dir, processed)
    process.process_cms_enrollment(raw_dir, processed)
    process.process_cms_plan_mix(raw_dir, processed)
    process.process_cms_stars(raw_dir, processed)

    out_dir = tmp_path / "states"
    web_dir = tmp_path / "web"
    web_dir.mkdir(parents=True, exist_ok=True)

    argv = [
        "build.py",
        "--processed", str(processed),
        "--out", str(out_dir),
        "--web-out", str(web_dir),
        "--roles", str(ROOT / "data" / "config" / "roles.yml"),
        "--date", "2026-02-07",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    build.main()
    return out_dir, web_dir, processed


def test_process_and_build_with_samples(tmp_path, monkeypatch):
    out_dir, web_dir, processed = run_full_pipeline(tmp_path, monkeypatch)

    expected_files = [
        "onc_state.csv",
        "ruca_state.csv",
        "cms_enrollment_state.csv",
        "cms_plan_mix_state.csv",
        "cms_stars_state.csv",
    ]
    for name in expected_files:
        assert (processed / name).exists(), f"missing {name}"

    index_path = out_dir / "index.json"
    assert index_path.exists(), "missing index.json"

    payload = json.loads(index_path.read_text(encoding="utf-8"))
    assert "states" in payload
    assert len(payload["states"]) == 6

    ca_path = out_dir / "CA.json"
    assert ca_path.exists(), "missing CA.json"

    ca_payload = json.loads(ca_path.read_text(encoding="utf-8"))
    mapd = ca_payload.get("mapd_pdp", {})
    assert "mapd_share_pct" in mapd
    assert "pdp_share_pct" in mapd
    assert "split_method" in mapd


def test_grammar_headline_article(tmp_path, monkeypatch):
    """Ensure 'an urban' not 'a urban' in generated headlines."""
    out_dir, _, _ = run_full_pipeline(tmp_path, monkeypatch)

    ca = json.loads((out_dir / "CA.json").read_text(encoding="utf-8"))
    headline = ca["summary"]["headline"]
    assert "an urban" in headline.lower(), f"Expected 'an urban' in: {headline}"
    assert "a urban" not in headline.lower(), f"Found 'a urban' in: {headline}"


def test_role_overrides_merge_with_defaults(tmp_path, monkeypatch):
    """Iowa and Florida should have all 6 roles, not just the overridden ones."""
    out_dir, _, _ = run_full_pipeline(tmp_path, monkeypatch)

    ia = json.loads((out_dir / "IA.json").read_text(encoding="utf-8"))
    ia_roles = ia["roles_impact"]["roles"]
    assert len(ia_roles) == 6, f"Iowa should have 6 roles, got {len(ia_roles)}"

    pe_role = next(r for r in ia_roles if r["role"] == "Provider Engagement")
    assert pe_role["impact"] == "High", f"Expected High for Provider Engagement in IA, got {pe_role['impact']}"

    fl = json.loads((out_dir / "FL.json").read_text(encoding="utf-8"))
    fl_roles = fl["roles_impact"]["roles"]
    assert len(fl_roles) == 6, f"Florida should have 6 roles, got {len(fl_roles)}"

    qa_role = next(r for r in fl_roles if r["role"] == "Quality Assurance")
    assert qa_role["impact"] == "High"
    assert "churn" in qa_role["why"].lower()


def test_onc_extracts_hie_and_patient_access(tmp_path):
    """The ONC processor should extract HIE and patient_access from legacy data."""
    raw_dir = copy_samples(tmp_path)
    processed = tmp_path / "processed"
    processed.mkdir()

    process.process_onc(raw_dir, processed)

    from utils import read_csv
    rows = read_csv(processed / "onc_state.csv")
    ca_row = next(r for r in rows if r["state"] == "CA")

    assert ca_row["hie_exchange_pct"] != "", f"HIE should be populated, got: {ca_row['hie_exchange_pct']}"
    assert ca_row["patient_access_pct"] != "", f"Patient access should be populated"
    assert float(ca_row["hie_exchange_pct"]) == 72.0
    assert float(ca_row["patient_access_pct"]) == 68.0


def test_iowa_rural_percentage(tmp_path):
    """Iowa should be classified as mixed or rural-heavy given representative sample data."""
    raw_dir = copy_samples(tmp_path)
    processed = tmp_path / "processed"
    processed.mkdir()

    process.process_ruca(raw_dir, processed)

    from utils import read_csv
    rows = read_csv(processed / "ruca_state.csv")
    ia_row = next(r for r in rows if r["state"] == "IA")

    rural_pct = float(ia_row["rural_pct"])
    assert rural_pct >= 20, f"Iowa rural % should be >= 20, got {rural_pct}"


def test_web_mirror_no_index_in_states(tmp_path, monkeypatch):
    """Build should not copy index.json into web/data/states/."""
    out_dir, web_dir, _ = run_full_pipeline(tmp_path, monkeypatch)

    assert (web_dir / "index.json").exists(), "index.json should be in web/data/"
    assert not (web_dir / "states" / "index.json").exists(), "index.json should NOT be in web/data/states/"


def test_six_states_in_samples(tmp_path, monkeypatch):
    """Sample data should produce 6 states: CA, FL, IA, NY, OH, TX."""
    out_dir, web_dir, _ = run_full_pipeline(tmp_path, monkeypatch)

    index = json.loads((out_dir / "index.json").read_text(encoding="utf-8"))
    codes = sorted(s["code"] for s in index["states"])
    assert codes == ["CA", "FL", "IA", "NY", "OH", "TX"], f"Expected 6 states, got {codes}"

    for code in codes:
        assert (out_dir / f"{code}.json").exists(), f"Missing {code}.json"
        assert (web_dir / "states" / f"{code}.json").exists(), f"Missing web mirror for {code}.json"


def test_readiness_score_uses_all_metrics(tmp_path):
    """Readiness score should incorporate all available metrics, not just EHR + API."""
    raw_dir = copy_samples(tmp_path)
    processed = tmp_path / "processed"
    processed.mkdir()

    process.process_onc(raw_dir, processed)

    from utils import read_csv
    rows = read_csv(processed / "onc_state.csv")
    ca_row = next(r for r in rows if r["state"] == "CA")

    # CA: ehr=91, hie=72, api=58, patient_access=68 -> mean = 72.25
    readiness = float(ca_row["readiness_score"])
    assert 72.0 <= readiness <= 73.0, f"Expected readiness ~72.25, got {readiness}"
