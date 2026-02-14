from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class Metric:
    label: str
    path: tuple[str, ...]
    expected_min: float | None = None
    expected_max: float | None = None


METRICS = [
    Metric("Readiness score", ("digital_readiness", "readiness_score"), 0, 100),
    Metric("Rural population share (%)", ("rural_urban", "rural_pct"), 0, 100),
    Metric("MAPD share (%)", ("mapd_pdp", "mapd_share_pct"), 0, 100),
    Metric("MA-only share (%)", ("mapd_pdp", "ma_only_share_pct"), 0, 100),
    Metric("PDP share (%)", ("mapd_pdp", "pdp_share_pct"), 0, 100),
    Metric("Average Star rating", ("stars_context", "avg_star"), 0, 5),
    Metric("Star volatility", ("stars_context", "volatility_index"), 0, 1),
    Metric("MA enrollment", ("stars_context", "ma_enrollment"), 0, None),
    Metric("Part D enrollment", ("stars_context", "partd_enrollment"), 0, None),
]


def read_csv_rows(path: Path) -> int:
    if not path.exists():
        return -1
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        rows = list(reader)
    return max(0, len(rows) - 1)


def get_path_value(data: dict, path: tuple[str, ...]) -> object | None:
    current: object = data
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def load_state_files(states_dir: Path) -> tuple[list[dict], list[str], list[str]]:
    index_path = states_dir / "index.json"
    if not index_path.exists():
        return [], [], ["Missing index.json in states directory."]
    index = json.loads(index_path.read_text())
    states = index.get("states", [])
    if not states:
        return [], [], ["index.json has no states."]
    missing = []
    data = []
    for entry in states:
        code = entry.get("code")
        if not code:
            continue
        state_path = states_dir / f"{code}.json"
        if not state_path.exists():
            missing.append(code)
            continue
        data.append(json.loads(state_path.read_text()))
    errors = []
    if missing:
        errors.append(f"Missing state JSON files: {', '.join(sorted(missing))}")
    return data, [s.get("code") for s in states if s.get("code")], errors


def compute_share_checks(state_data: list[dict]) -> list[str]:
    warnings = []
    for state in state_data:
        code = state.get("state", {}).get("code", "UNKNOWN")
        mapd = get_path_value(state, ("mapd_pdp", "mapd_share_pct"))
        ma_only = get_path_value(state, ("mapd_pdp", "ma_only_share_pct"))
        pdp = get_path_value(state, ("mapd_pdp", "pdp_share_pct"))
        if mapd is None or pdp is None:
            continue
        if ma_only is None:
            total = mapd + pdp
        else:
            total = mapd + ma_only + pdp
        if abs(total - 100) > 1.0:
            warnings.append(f"{code}: plan mix shares sum to {total:.2f} (expected ~100).")
    return warnings


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Run QA checks on pipeline outputs.")
    parser.add_argument("--states-dir", default=str(root / "data" / "states"))
    parser.add_argument("--processed-dir", default=str(root / "data" / "processed"))
    parser.add_argument("--out", default=str(root / "reports" / "qa"))
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()

    states_dir = Path(args.states_dir)
    processed_dir = Path(args.processed_dir)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    report_path = out_dir / f"qa_{args.date}.md"

    processed_files = [
        "onc_state.csv",
        "ruca_state.csv",
        "cms_enrollment_state.csv",
        "cms_plan_mix_state.csv",
        "cms_stars_state.csv",
    ]
    processed_rows = {name: read_csv_rows(processed_dir / name) for name in processed_files}

    state_data, state_codes, errors = load_state_files(states_dir)

    missing_by_metric: dict[str, list[str]] = {}
    range_warnings: list[str] = []
    for metric in METRICS:
        missing_states = []
        for state in state_data:
            code = state.get("state", {}).get("code", "UNKNOWN")
            value = get_path_value(state, metric.path)
            if value is None:
                missing_states.append(code)
                continue
            if isinstance(value, (int, float)) and metric.expected_min is not None:
                if value < metric.expected_min:
                    range_warnings.append(f"{code}: {metric.label} below {metric.expected_min}.")
            if isinstance(value, (int, float)) and metric.expected_max is not None:
                if value > metric.expected_max:
                    range_warnings.append(f"{code}: {metric.label} above {metric.expected_max}.")
        if missing_states:
            missing_by_metric[metric.label] = sorted(missing_states)

    share_warnings = compute_share_checks(state_data)

    status = "PASS"
    if errors:
        status = "FAIL"
    elif missing_by_metric:
        status = "WARN"
    elif share_warnings or range_warnings:
        status = "WARN"

    lines = []
    lines.append("# QA Report")
    lines.append("")
    lines.append(f"Date: {args.date}")
    lines.append(f"Status: {status}")
    lines.append("")
    lines.append("## Processed Tables")
    for name, rows in processed_rows.items():
        if rows == -1:
            lines.append(f"- {name}: MISSING")
        else:
            lines.append(f"- {name}: {rows} data rows")
    lines.append("")
    lines.append("## State Artifacts")
    if not state_codes:
        lines.append("- No states found in index.json")
    else:
        lines.append(f"- States in index.json: {len(state_codes)}")
        lines.append(f"- State JSON files loaded: {len(state_data)}")
    if errors:
        lines.append("")
        lines.append("## Errors")
        for err in errors:
            lines.append(f"- {err}")

    if missing_by_metric:
        lines.append("")
        lines.append("## Missing Data")
        for label, states in missing_by_metric.items():
            lines.append(f"- {label}: {len(states)} missing ({', '.join(states)})")

    if range_warnings or share_warnings:
        lines.append("")
        lines.append("## Warnings")
        for warning in range_warnings + share_warnings:
            lines.append(f"- {warning}")

    report_path.write_text("\n".join(lines) + "\n")

    if status == "FAIL":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
