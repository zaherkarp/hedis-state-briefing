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


def test_process_and_build_with_samples(tmp_path, monkeypatch):
    raw_dir = copy_samples(tmp_path)
    processed = tmp_path / "processed"
    processed.mkdir()

    process.process_onc(raw_dir, processed)
    process.process_ruca(raw_dir, processed)
    process.process_cms_enrollment(raw_dir, processed)
    process.process_cms_plan_mix(raw_dir, processed)
    process.process_cms_stars(raw_dir, processed)

    expected_files = [
        "onc_state.csv",
        "ruca_state.csv",
        "cms_enrollment_state.csv",
        "cms_plan_mix_state.csv",
        "cms_stars_state.csv",
    ]
    for name in expected_files:
        assert (processed / name).exists(), f"missing {name}"

    out_dir = tmp_path / "states"
    web_dir = tmp_path / "web"
    web_dir.mkdir(parents=True, exist_ok=True)

    argv = [
        "build.py",
        "--processed",
        str(processed),
        "--out",
        str(out_dir),
        "--web-out",
        str(web_dir),
        "--roles",
        str(ROOT / "data" / "config" / "roles.yml"),
        "--date",
        "2026-02-07",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    build.main()

    index_path = out_dir / "index.json"
    assert index_path.exists(), "missing index.json"

    payload = json.loads(index_path.read_text(encoding="utf-8"))
    assert "states" in payload
    assert len(payload["states"]) == 3

    ca_path = out_dir / "CA.json"
    assert ca_path.exists(), "missing CA.json"

    ca_payload = json.loads(ca_path.read_text(encoding="utf-8"))
    mapd = ca_payload.get("mapd_pdp", {})
    assert "mapd_share_pct" in mapd
    assert "pdp_share_pct" in mapd
    assert "split_method" in mapd
