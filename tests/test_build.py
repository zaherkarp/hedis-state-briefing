import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import build


def test_readiness_label_thresholds():
    assert build.readiness_label(None) == "Unknown readiness"
    assert build.readiness_label(80.0) == "Higher readiness"
    assert build.readiness_label(70.0) == "Higher readiness"
    assert build.readiness_label(60.0) == "Mixed readiness"
    assert build.readiness_label(55.0) == "Mixed readiness"
    assert build.readiness_label(40.0) == "Lower readiness"
    assert build.readiness_label(0.0) == "Lower readiness"


def test_rural_label_thresholds():
    assert build.rural_label(None) == "Unknown rural mix"
    assert build.rural_label(50.0) == "Rural-heavy"
    assert build.rural_label(40.0) == "Rural-heavy"
    assert build.rural_label(30.0) == "Mixed rural/urban"
    assert build.rural_label(20.0) == "Mixed rural/urban"
    assert build.rural_label(10.0) == "Urban-heavy"


def test_plan_mix_label_mapd_split():
    assert build.plan_mix_label(None, "cpsc") == "Unknown plan mix"
    assert build.plan_mix_label(80.0, "cpsc") == "MAPD-dominant"
    assert build.plan_mix_label(70.0, "cpsc") == "MAPD-dominant"
    assert build.plan_mix_label(50.0, "cpsc") == "Balanced MAPD/PDP"
    assert build.plan_mix_label(40.0, "cpsc") == "PDP-leaning"
    assert build.plan_mix_label(30.0, "cpsc") == "PDP-leaning"


def test_plan_mix_label_ma_vs_pdp_split():
    assert build.plan_mix_label(80.0, "ma_vs_pdp") == "MA-dominant"
    assert build.plan_mix_label(50.0, "ma_vs_pdp") == "Balanced MA/PDP"
    assert build.plan_mix_label(30.0, "ma_vs_pdp") == "PDP-leaning"


def test_volatility_label_thresholds():
    assert build.volatility_label(None) == "Unknown volatility"
    assert build.volatility_label(0.5) == "Higher volatility"
    assert build.volatility_label(0.4) == "Higher volatility"
    assert build.volatility_label(0.35) == "Moderate volatility"
    assert build.volatility_label(0.3) == "Moderate volatility"
    assert build.volatility_label(0.2) == "Lower volatility"


def test_pick_roles_defaults():
    config = {
        "defaults": [
            {"role": "Data Engineering", "impact": "High", "why": "Pipelines"},
        ],
        "state_overrides": {},
    }
    roles = build.pick_roles(config, "CA")
    assert len(roles) == 1
    assert roles[0]["role"] == "Data Engineering"


def test_pick_roles_state_override():
    config = {
        "defaults": [
            {"role": "Data Engineering", "impact": "High", "why": "Pipelines"},
        ],
        "state_overrides": {
            "IA": [
                {"role": "QA", "impact": "High", "why": "Rural gaps"},
            ],
        },
    }
    roles = build.pick_roles(config, "IA")
    assert len(roles) == 1
    assert roles[0]["role"] == "QA"


def test_build_state_payload_structure():
    payload = build.build_state_payload(
        state="CA",
        onc={"readiness_score": "72.5", "ehr_adoption_pct": "90.0", "hie_exchange_pct": "65.0", "api_use_pct": "45.0"},
        ruca={"rural_pct": "5.0", "urban_pct": "95.0"},
        enrollment={"ma_enrollment": "4500000", "partd_enrollment": "3000000"},
        plan_mix={"mapd_share_pct": "75.0", "pdp_share_pct": "20.0", "ma_only_share_pct": "5.0", "split_method": "cpsc"},
        stars={"avg_star": "3.8", "volatility_index": "0.25", "churn_pct": "12.5"},
        roles_config={"defaults": [{"role": "DE", "impact": "High", "why": "test"}]},
        updated_at="2026-01-01",
    )

    assert payload["state"]["code"] == "CA"
    assert payload["state"]["name"] == "California"
    assert payload["updated_at"] == "2026-01-01"

    assert "headline" in payload["summary"]
    assert "California" in payload["summary"]["headline"]
    assert len(payload["summary"]["key_points"]) == 3

    assert payload["digital_readiness"]["readiness_score"] == 72.5
    assert payload["digital_readiness"]["readiness_label"] == "Higher readiness"

    assert payload["rural_urban"]["rural_pct"] == 5.0
    assert payload["rural_urban"]["label"] == "Urban-heavy"

    assert payload["mapd_pdp"]["mapd_share_pct"] == 75.0
    assert payload["mapd_pdp"]["split_method"] == "cpsc"
    assert payload["mapd_pdp"]["label"] == "MAPD-dominant"
    assert payload["mapd_pdp"]["method_note"] is None

    assert payload["stars_context"]["avg_star"] == 3.8
    assert payload["stars_context"]["volatility_label"] == "Lower volatility"

    assert len(payload["roles_impact"]["roles"]) == 1
    assert len(payload["preseason_shift"]["before"]) == 3
    assert len(payload["preseason_shift"]["after"]) == 3


def test_build_state_payload_missing_data():
    payload = build.build_state_payload(
        state="XX",
        onc={},
        ruca={},
        enrollment={},
        plan_mix={},
        stars={},
        roles_config={"defaults": []},
        updated_at="2026-01-01",
    )

    assert payload["state"]["code"] == "XX"
    assert payload["state"]["name"] == "XX"
    assert payload["digital_readiness"]["readiness_score"] is None
    assert payload["digital_readiness"]["readiness_label"] == "Unknown readiness"
    assert payload["rural_urban"]["rural_pct"] is None
    assert payload["mapd_pdp"]["mapd_share_pct"] is None
    assert payload["stars_context"]["avg_star"] is None


def test_build_state_payload_rural_heavy_adds_risk():
    payload = build.build_state_payload(
        state="IA",
        onc={"readiness_score": "50"},
        ruca={"rural_pct": "45", "urban_pct": "55"},
        enrollment={},
        plan_mix={},
        stars={},
        roles_config={"defaults": []},
        updated_at="2026-01-01",
    )

    risks = payload["preseason_shift"]["operational_risks"]
    assert any("Rural" in r for r in risks)


def test_build_state_payload_ma_vs_pdp_method():
    payload = build.build_state_payload(
        state="FL",
        onc={},
        ruca={},
        enrollment={"mapd_share_pct": "60"},
        plan_mix={"split_method": "ma_vs_pdp"},
        stars={},
        roles_config={"defaults": []},
        updated_at="2026-01-01",
    )

    assert payload["mapd_pdp"]["split_method"] == "ma_vs_pdp"
    assert payload["mapd_pdp"]["method_note"] is not None
    assert "MA" in payload["mapd_pdp"]["implications"][0]
