from __future__ import annotations

import argparse
import csv
import shutil
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List

import requests
import yaml


def load_config(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def download(url: str, dest: Path, force: bool) -> None:
    if dest.exists() and not force:
        print(f"skip (exists): {dest}")
        return
    print(f"fetch: {url}")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(response.content)
    print(f"saved: {dest}")


def copy_samples(samples_dir: Path, out_dir: Path) -> None:
    if not samples_dir.exists():
        print(f"sample dir missing: {samples_dir}")
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    for sample in samples_dir.glob("*.csv"):
        target = out_dir / sample.name
        shutil.copy2(sample, target)
        print(f"copied sample: {target}")


def fetch_open_api_csv(item: Dict[str, Any], out_dir: Path, force: bool) -> None:
    filename = item.get("filename")
    api_base = item.get("api_base")
    source = item.get("source")
    states = item.get("states", [])
    state_params = item.get("state_params") or ["State", "state"]

    if not filename or not api_base or not source or not states:
        print(f"skip (missing api config): {item.get('id')}")
        return

    dest = out_dir / filename
    if dest.exists() and not force:
        print(f"skip (exists): {dest}")
        return

    rows: List[Dict[str, str]] = []
    for state in states:
        success = False
        for param in state_params:
            params = {"source": source, "format": "csv", param: state}
            response = requests.get(api_base, params=params, timeout=60)
            if response.status_code != 200:
                continue
            text = response.text.strip()
            if not text:
                continue
            reader = csv.DictReader(StringIO(text))
            batch = [row for row in reader]
            if not batch:
                continue
            for row in batch:
                if not any(key.lower() == "state" for key in row.keys()):
                    row["state"] = state
                rows.append(row)
            success = True
            break
        if not success:
            print(f"no data for state: {state} ({item.get('id')})")

    if not rows:
        print(f"no rows returned for {item.get('id')}")
        return

    fieldnames = list(rows[0].keys())
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"saved api data: {dest}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch public datasets into data/raw.")
    parser.add_argument("--config", default="data/config/sources.yml")
    parser.add_argument("--out", default="data/raw")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--use-samples", action="store_true", help="Copy sample data into data/raw instead of downloading.")
    parser.add_argument("--allow-disabled", action="store_true", help="Download sources even if enabled=false.")
    args = parser.parse_args()

    config_path = Path(args.config)
    out_dir = Path(args.out)

    if args.use_samples:
        copy_samples(Path("data/samples/raw"), out_dir)
        return

    config = load_config(config_path)
    for group_name, group in config.items():
        enabled = bool(group.get("enabled"))
        if not enabled and not args.allow_disabled:
            print(f"skip (disabled): {group_name}")
            continue
        for item in group.get("files", []):
            if item.get("api_base"):
                fetch_open_api_csv(item, out_dir, args.force)
                continue
            url = item.get("url")
            filename = item.get("filename")
            if not url or url == "REPLACE_ME":
                print(f"skip (missing url): {group_name}:{item.get('id')}")
                continue
            if not filename:
                print(f"skip (missing filename): {group_name}:{item.get('id')}")
                continue
            dest = out_dir / filename
            download(url, dest, args.force)


if __name__ == "__main__":
    main()
