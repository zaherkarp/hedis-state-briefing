from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def rank_top(items: List[Tuple[str, Optional[float]]], top_n: int) -> List[Tuple[str, float]]:
    filtered = [(state, value) for state, value in items if value is not None]
    filtered.sort(key=lambda item: item[1], reverse=True)
    return [(state, value) for state, value in filtered[:top_n]]


def collect_states(states_dir: Path) -> List[Dict[str, Any]]:
    index_path = states_dir / "index.json"
    if not index_path.exists():
        raise FileNotFoundError("index.json not found. Run build.py first.")
    index = load_json(index_path)
    states = []
    for entry in index.get("states", []):
        code = entry.get("code")
        if not code:
            continue
        state_path = states_dir / f"{code}.json"
        if not state_path.exists():
            continue
        states.append(load_json(state_path))
    return states


def metric_value(state: Dict[str, Any], path: List[str]) -> Optional[float]:
    current: Any = state
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return safe_float(current)


def generate_report(states: List[Dict[str, Any]], top_n: int) -> str:
    top_sections = []
    missing_sections = []

    metrics = {
        "ECDS readiness (score)": ["digital_readiness", "readiness_score"],
        "Rural population share (%)": ["rural_urban", "rural_pct"],
        "MAPD share (%)": ["mapd_pdp", "mapd_share_pct"],
        "MA-only share (%)": ["mapd_pdp", "ma_only_share_pct"],
        "PDP share (%)": ["mapd_pdp", "pdp_share_pct"],
        "Average Star rating": ["stars_context", "avg_star"],
        "Star volatility": ["stars_context", "volatility_index"],
        "MA enrollment": ["stars_context", "ma_enrollment"],
        "Part D enrollment": ["stars_context", "partd_enrollment"],
    }

    for label, path in metrics.items():
        values = []
        missing = []
        for state in states:
            code = state.get("state", {}).get("code", "")
            value = metric_value(state, path)
            if value is None:
                missing.append(code)
            else:
                values.append((code, value))
        top_items = rank_top(values, top_n)
        top_lines = [f"- {code}: {value:.2f}" for code, value in top_items]
        if not top_lines:
            top_lines = ["- No data"]
        top_sections.append(f"### {label}\n" + "\n".join(top_lines))
        if missing:
            missing_lines = "\n".join([f"- {code}" for code in sorted(missing)])
            missing_sections.append(f"### Missing {label}\n" + missing_lines)

    return "\n\n".join([
        "## Coverage Report",
        f"Generated: {date.today().isoformat()}",
        "\n".join(top_sections),
        "## Missing Data Coverage",
        "\n".join(missing_sections) if missing_sections else "All metrics populated.",
    ])


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a coverage report for state briefing outputs.")
    parser.add_argument("--states-dir", default="data/states")
    parser.add_argument("--out", default="reports/coverage")
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--date", default=None)
    args = parser.parse_args()

    states_dir = Path(args.states_dir)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    report = generate_report(collect_states(states_dir), args.top)
    stamp = args.date or date.today().isoformat()
    out_path = out_dir / f"coverage_{stamp}.md"
    out_path.write_text(report, encoding="utf-8")
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
