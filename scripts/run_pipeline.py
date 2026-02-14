from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import build
import coverage_report
import fetch
import process
import qa_checks


def run_module(main_func, argv: list[str]) -> None:
    original = sys.argv
    try:
        sys.argv = argv
        main_func()
    finally:
        sys.argv = original


def main() -> None:
    root = Path(__file__).resolve().parents[1]

    parser = argparse.ArgumentParser(description="Run fetch -> process -> build -> coverage report -> QA checks.")
    parser.add_argument("--config", default=str(root / "data" / "config" / "sources.yml"))
    parser.add_argument("--raw", default=str(root / "data" / "raw"))
    parser.add_argument("--processed", default=str(root / "data" / "processed"))
    parser.add_argument("--states", default=str(root / "data" / "states"))
    parser.add_argument("--web-out", default=str(root / "web" / "data"))
    parser.add_argument("--roles", default=str(root / "data" / "config" / "roles.yml"))
    parser.add_argument("--coverage-out", default=str(root / "reports" / "coverage"))
    parser.add_argument("--qa-out", default=str(root / "reports" / "qa"))
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--use-samples", action="store_true")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip the fetch step.")
    parser.add_argument("--skip-qa", action="store_true", help="Skip QA checks.")
    parser.add_argument("--allow-disabled", action="store_true", help="Fetch sources even if enabled=false.")
    parser.add_argument("--force-fetch", action="store_true", help="Re-download raw files.")
    args = parser.parse_args()

    if not args.skip_fetch:
        fetch_args = ["fetch.py", "--config", args.config, "--out", args.raw]
        if args.use_samples:
            fetch_args.append("--use-samples")
        if args.allow_disabled:
            fetch_args.append("--allow-disabled")
        if args.force_fetch:
            fetch_args.append("--force")
        run_module(fetch.main, fetch_args)

    process_args = ["process.py", "--raw", args.raw, "--processed", args.processed]
    if args.use_samples:
        process_args.append("--use-samples")
    run_module(process.main, process_args)

    build_args = [
        "build.py",
        "--processed",
        args.processed,
        "--out",
        args.states,
        "--web-out",
        args.web_out,
        "--roles",
        args.roles,
        "--date",
        args.date,
    ]
    run_module(build.main, build_args)

    coverage_args = [
        "coverage_report.py",
        "--states-dir",
        args.states,
        "--out",
        args.coverage_out,
        "--top",
        str(args.top),
        "--date",
        args.date,
    ]
    run_module(coverage_report.main, coverage_args)

    if not args.skip_qa:
        qa_args = [
            "qa_checks.py",
            "--states-dir",
            args.states,
            "--processed-dir",
            args.processed,
            "--out",
            args.qa_out,
            "--date",
            args.date,
        ]
        run_module(qa_checks.main, qa_args)


if __name__ == "__main__":
    main()
