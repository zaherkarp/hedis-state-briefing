from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from utils import parse_float, parse_int, read_csv, write_json

STATE_NAMES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
}


def readiness_label(score: Optional[float]) -> str:
    if score is None:
        return "Unknown readiness"
    if score >= 70:
        return "Higher readiness"
    if score >= 55:
        return "Mixed readiness"
    return "Lower readiness"


def rural_label(rural_pct: Optional[float]) -> str:
    if rural_pct is None:
        return "Unknown rural mix"
    if rural_pct >= 40:
        return "Rural-heavy"
    if rural_pct >= 20:
        return "Mixed rural/urban"
    return "Urban-heavy"


def plan_mix_label(mapd_share: Optional[float], split_method: str) -> str:
    if mapd_share is None:
        return "Unknown plan mix"
    if split_method == "ma_vs_pdp":
        if mapd_share >= 70:
            return "MA-dominant"
        if mapd_share <= 40:
            return "PDP-leaning"
        return "Balanced MA/PDP"
    if mapd_share >= 70:
        return "MAPD-dominant"
    if mapd_share <= 40:
        return "PDP-leaning"
    return "Balanced MAPD/PDP"


def volatility_label(volatility: Optional[float]) -> str:
    if volatility is None:
        return "Unknown volatility"
    if volatility >= 0.4:
        return "Higher volatility"
    if volatility >= 0.3:
        return "Moderate volatility"
    return "Lower volatility"


def load_roles(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def pick_roles(roles_config: Dict[str, Any], state: str) -> List[Dict[str, str]]:
    overrides = roles_config.get("state_overrides", {}).get(state)
    if overrides:
        return overrides
    return roles_config.get("defaults", [])


def build_state_payload(
    state: str,
    onc: Dict[str, str],
    ruca: Dict[str, str],
    enrollment: Dict[str, str],
    plan_mix: Dict[str, str],
    stars: Dict[str, str],
    roles_config: Dict[str, Any],
    updated_at: str,
) -> Dict[str, Any]:
    name = STATE_NAMES.get(state, state)

    readiness_score = parse_float(onc.get("readiness_score"))
    readiness = readiness_label(readiness_score)

    rural_pct = parse_float(ruca.get("rural_pct"))
    rural_mix = rural_label(rural_pct)

    mapd_share = parse_float(plan_mix.get("mapd_share_pct") or enrollment.get("mapd_share_pct"))
    pdp_share = parse_float(plan_mix.get("pdp_share_pct") or enrollment.get("pdp_share_pct"))
    ma_only_share = parse_float(plan_mix.get("ma_only_share_pct"))
    split_method = plan_mix.get("split_method") or ("ma_vs_pdp" if mapd_share is not None else "unknown")
    plan_mix_label_text = plan_mix_label(mapd_share, split_method)

    volatility = parse_float(stars.get("volatility_index"))
    volatility_note = volatility_label(volatility)

    key_points: List[str] = []
    if readiness_score is not None:
        if readiness_score < 55:
            key_points.append("Interoperability gaps will slow early ECDS validation and measure sign-off.")
        elif readiness_score < 70:
            key_points.append("Readiness is uneven; target high-variance workflows first.")
        else:
            key_points.append("Strong digital readiness allows earlier ECDS pilots and QA cycles.")
    else:
        key_points.append("ECDS readiness signals are not yet available; prioritize local assessment.")

    if rural_pct is not None:
        if rural_pct >= 40:
            key_points.append("Rural capacity constraints will show up as data lag and staffing stretch.")
        elif rural_pct >= 20:
            key_points.append("Mixed rural/urban footprint requires dual-track enablement plans.")
        else:
            key_points.append("Urban-heavy footprint supports faster data iteration but higher volume risk.")
    else:
        key_points.append("Rural/urban mix unknown; validate connectivity and staffing constraints.")

    if mapd_share is not None:
        if split_method == "ma_vs_pdp":
            if mapd_share >= 70:
                key_points.append("MA performance will drive most Stars exposure in this state.")
            elif mapd_share <= 40:
                key_points.append("PDP exposure remains meaningful; Part D workflows need equal attention.")
            else:
                key_points.append("Balanced MA/PDP mix requires parallel operational focus.")
        else:
            if mapd_share >= 70:
                key_points.append("MAPD performance will drive most Stars exposure in this state.")
            elif mapd_share <= 40:
                key_points.append("PDP exposure remains meaningful; Part D workflows need equal attention.")
            else:
                key_points.append("Balanced MAPD/PDP mix requires parallel operational focus.")
    else:
        key_points.append("Plan mix unknown; confirm MAPD vs PDP exposure early.")

    headline = f"{readiness} for ECDS in {name} with a {rural_mix.lower()} operating context."
    subheadline = "Pre-season work shifts earlier with heavier data validation and cross-team coordination."

    preseason_before = [
        "Late-fall data pulls with limited back-and-forth.",
        "Measure owners validate after upstream extraction is mostly complete.",
        "QA cycles run close to submission windows.",
    ]
    preseason_after = [
        "Early-fall data readiness checks and ECDS validation rounds.",
        "Measure logic alignment begins earlier with more dependencies.",
        "QA cycles expand to cover new data interfaces and edge cases.",
    ]
    operational_risks = [
        "Data quality issues surface earlier and require rapid remediation.",
        "Capacity pinch for data engineering and QA during pre-season.",
        "Higher coordination load across measure owners, analytics, and ops.",
    ]
    if rural_pct is not None and rural_pct >= 40:
        operational_risks.append("Rural site connectivity and staffing gaps extend the validation window.")

    roles = pick_roles(roles_config, state)

    method_note = None
    if split_method == "ma_vs_pdp":
        method_note = "MAPD vs MA-only split not available; MAPD share reflects total MA enrollment."

    implications = [
        "MAPD incentives will dominate the performance story when MAPD share is high.",
        "PDP workflows remain critical when PDP share is material.",
    ]
    if split_method == "ma_vs_pdp":
        implications = [
            "MA incentives will dominate the performance story when MA share is high.",
            "PDP workflows remain critical when PDP share is material.",
        ]

    payload = {
        "state": {"code": state, "name": name},
        "updated_at": updated_at,
        "summary": {
            "headline": headline,
            "subheadline": subheadline,
            "key_points": key_points,
        },
        "digital_readiness": {
            "reporting_year": onc.get("reporting_year"),
            "readiness_score": readiness_score,
            "readiness_label": readiness,
            "ehr_adoption_pct": parse_float(onc.get("ehr_adoption_pct")),
            "hie_exchange_pct": parse_float(onc.get("hie_exchange_pct")),
            "patient_access_pct": parse_float(onc.get("patient_access_pct")),
            "tefca_ready_pct": parse_float(onc.get("tefca_ready_pct")),
            "api_use_pct": parse_float(onc.get("api_use_pct")),
            "insight": "ECDS readiness is driven by interoperability and patient access signals.",
        },
        "rural_urban": {
            "rural_pct": rural_pct,
            "urban_pct": parse_float(ruca.get("urban_pct")),
            "label": rural_mix,
            "constraints": [
                "Staffing variability and connectivity gaps extend validation cycles.",
                "Smaller clinics create higher variance in data completeness.",
            ],
            "implications": [
                "Phase pilots by network maturity, not just geography.",
                "Plan extra enablement time for rural sites.",
            ],
        },
        "mapd_pdp": {
            "mapd_share_pct": mapd_share,
            "ma_only_share_pct": ma_only_share,
            "pdp_share_pct": pdp_share,
            "label": plan_mix_label_text,
            "split_method": split_method,
            "method_note": method_note,
            "implications": implications,
        },
        "roles_impact": {
            "summary": "Data engineering, QA, and measure owners carry the earliest burden.",
            "roles": roles,
        },
        "preseason_shift": {
            "before": preseason_before,
            "after": preseason_after,
            "operational_risks": operational_risks,
        },
        "stars_context": {
            "reporting_year": stars.get("reporting_year") or enrollment.get("reporting_year"),
            "ma_enrollment": parse_int(enrollment.get("ma_enrollment")),
            "partd_enrollment": parse_int(enrollment.get("partd_enrollment")),
            "avg_star": parse_float(stars.get("avg_star")),
            "volatility_index": volatility,
            "volatility_label": volatility_note,
            "churn_pct": parse_float(stars.get("churn_pct")),
            "notes": [
                "Enrollment size and churn drive operational exposure.",
                "Higher volatility signals more unstable contract performance.",
            ],
        },
        "sources": {
            "onc": "ONC Health IT Dashboard",
            "cms": "CMS MA/Part D",
            "ruca": "USDA ERS RUCA",
            "census": "Optional Census population context",
        },
        "future": {
            "organizations": [],
            "interviews": [],
            "role_risk_scores": [],
        },
    }

    payload["summary"]["key_points"] = key_points
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build per-state JSON artifacts for the wall.")
    parser.add_argument("--processed", default="data/processed")
    parser.add_argument("--out", default="data/states")
    parser.add_argument("--web-out", default="web/data")
    parser.add_argument("--roles", default="data/config/roles.yml")
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()

    processed_dir = Path(args.processed)
    out_dir = Path(args.out)
    web_dir = Path(args.web_out)

    onc_rows = read_csv(processed_dir / "onc_state.csv")
    ruca_rows = read_csv(processed_dir / "ruca_state.csv")
    enrollment_rows = read_csv(processed_dir / "cms_enrollment_state.csv")
    plan_mix_rows = read_csv(processed_dir / "cms_plan_mix_state.csv")
    stars_rows = read_csv(processed_dir / "cms_stars_state.csv")

    onc_by_state = {row["state"].strip(): row for row in onc_rows if row.get("state")}
    ruca_by_state = {row["state"].strip(): row for row in ruca_rows if row.get("state")}
    enrollment_by_state = {row["state"].strip(): row for row in enrollment_rows if row.get("state")}
    plan_mix_by_state = {row["state"].strip(): row for row in plan_mix_rows if row.get("state")}
    stars_by_state = {row["state"].strip(): row for row in stars_rows if row.get("state")}

    all_states = sorted(
        set(onc_by_state)
        | set(ruca_by_state)
        | set(enrollment_by_state)
        | set(plan_mix_by_state)
        | set(stars_by_state)
    )

    roles_config = load_roles(Path(args.roles))

    index_payload: List[Dict[str, str]] = []
    for state in all_states:
        payload = build_state_payload(
            state,
            onc_by_state.get(state, {}),
            ruca_by_state.get(state, {}),
            enrollment_by_state.get(state, {}),
            plan_mix_by_state.get(state, {}),
            stars_by_state.get(state, {}),
            roles_config,
            args.date,
        )
        out_path = out_dir / f"{state}.json"
        write_json(out_path, payload)

        index_payload.append(
            {
                "code": state,
                "name": payload["state"]["name"],
                "headline": payload["summary"]["headline"],
            }
        )

    write_json(out_dir / "index.json", {"states": index_payload, "updated_at": args.date})

    # Mirror into web/data for static use
    if out_dir.exists():
        web_states_dir = web_dir / "states"
        web_states_dir.mkdir(parents=True, exist_ok=True)
        for state_file in out_dir.glob("*.json"):
            target = web_states_dir / state_file.name
            target.write_bytes(state_file.read_bytes())
        (web_dir / "index.json").write_bytes((out_dir / "index.json").read_bytes())

    print(f"built {len(all_states)} state files")


if __name__ == "__main__":
    main()
