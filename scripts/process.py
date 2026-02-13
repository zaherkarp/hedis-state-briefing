from __future__ import annotations

import argparse
import re
from pathlib import Path
from statistics import pstdev
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from zipfile import ZipFile

from utils import (
    mean,
    parse_float,
    parse_int,
    pick_column,
    read_csv,
    read_csv_from_zip,
    read_excel_from_zip,
    write_csv,
)

STATE_ABBR = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
    "DISTRICT OF COLUMBIA": "DC",
}


def normalize_state(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return cleaned
    if len(cleaned) == 2:
        return cleaned.upper()
    upper = cleaned.upper()
    return STATE_ABBR.get(upper, cleaned)


def infer_year(text: str) -> Optional[int]:
    match = re.search(r"(20\d{2})", text)
    if match:
        return int(match.group(1))
    return None


def select_zip_member(zip_path: Path, keywords: Sequence[str], allowed_ext: Sequence[str]) -> Optional[str]:
    if not zip_path.exists():
        return None
    with ZipFile(zip_path, "r") as handle:
        members = [info for info in handle.infolist() if info.filename.lower().endswith(tuple(allowed_ext))]
        if not members:
            return None
        scored = []
        for info in members:
            name = info.filename.lower()
            score = sum(1 for keyword in keywords if keyword in name)
            scored.append((score, info.file_size, info.filename))
        scored.sort(reverse=True)
        return scored[0][2]


def pick_numeric_column(headers: Sequence[str], rows: List[Dict[str, str]], exclude: Iterable[str]) -> Optional[str]:
    exclude_lower = {item.lower() for item in exclude}
    best_col = None
    best_count = 0
    for header in headers:
        if header.lower() in exclude_lower:
            continue
        count = 0
        for row in rows[:200]:
            value = parse_float(row.get(header))
            if value is not None:
                count += 1
        if count > best_count:
            best_count = count
            best_col = header
    return best_col


def extract_state_metric(
    rows: List[Dict[str, str]],
    state_candidates: Sequence[str],
    value_candidates: Sequence[str],
    year_candidates: Sequence[str],
) -> Dict[str, Dict[str, Optional[float]]]:
    if not rows:
        return {}
    headers = list(rows[0].keys())
    state_col = pick_column(headers, state_candidates)
    year_col = pick_column(headers, year_candidates)
    value_col = pick_column(headers, value_candidates)
    if not value_col:
        value_col = pick_numeric_column(headers, rows, exclude=[state_col or "", year_col or ""])
    if not state_col or not value_col:
        return {}

    output: Dict[str, Dict[str, Optional[float]]] = {}
    for row in rows:
        state = normalize_state(str(row.get(state_col, "")))
        if not state:
            continue
        value = parse_float(row.get(value_col))
        if value is None:
            continue
        year_value = row.get(year_col) if year_col else ""
        output[state] = {"value": value, "year": year_value}
    return output


def extract_erx_metric(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, Optional[float]]]:
    if not rows:
        return {}
    headers = list(rows[0].keys())
    state_col = pick_column(headers, ["state", "region", "region_name", "state_name", "state abbreviation"]) or ""
    year_col = pick_column(headers, ["year", "period", "reporting_year"]) or ""
    pct_col = pick_column(headers, ["pct_e_rx", "percent_e_rx", "percentage", "pct", "percent"])
    erx_col = pick_column(headers, ["tot_e_rx", "tot_erx", "e_rx", "erx"])
    total_col = pick_column(headers, ["tot_rx", "total_rx", "total", "rx_total"])

    output: Dict[str, Dict[str, Optional[float]]] = {}
    for row in rows:
        state = normalize_state(str(row.get(state_col, "")))
        if not state:
            continue
        pct_value = parse_float(row.get(pct_col)) if pct_col else None
        if pct_value is None and erx_col and total_col:
            erx_value = parse_float(row.get(erx_col))
            total_value = parse_float(row.get(total_col))
            if erx_value is not None and total_value:
                pct_value = round((erx_value / total_value) * 100, 1)
        if pct_value is None:
            continue
        year_value = row.get(year_col) if year_col else ""
        output[state] = {"value": pct_value, "year": year_value}
    return output


def process_onc(raw_dir: Path, out_dir: Path) -> None:
    legacy_ehr = read_csv(raw_dir / "onc_ehr_adoption.csv")
    legacy_interop = read_csv(raw_dir / "onc_interoperability.csv")

    api_ehr = read_csv(raw_dir / "onc_basic_ehr_by_state_api.csv")
    api_erx = read_csv(raw_dir / "onc_surescripts_erx_state_api.csv")

    # Extract per-metric lookups from legacy CSV columns when available
    hie_lookup: Dict[str, Dict[str, Optional[float]]] = {}
    patient_access_lookup: Dict[str, Dict[str, Optional[float]]] = {}

    if legacy_ehr:
        ehr_lookup = extract_state_metric(
            legacy_ehr,
            ["state"],
            ["ehr_adoption_pct"],
            ["reporting_year", "year", "period"],
        )
        # Legacy file may also carry hie_exchange_pct and patient_access_pct columns
        hie_lookup = extract_state_metric(
            legacy_ehr,
            ["state"],
            ["hie_exchange_pct"],
            ["reporting_year", "year", "period"],
        )
        patient_access_lookup = extract_state_metric(
            legacy_ehr,
            ["state"],
            ["patient_access_pct"],
            ["reporting_year", "year", "period"],
        )
    elif api_ehr:
        ehr_lookup = extract_state_metric(
            api_ehr,
            ["state", "state name", "region"],
            ["basic_ehr", "basic", "ehr", "adoption", "percent", "pct"],
            ["year", "period", "reporting_year"],
        )
    else:
        ehr_rows = read_csv(raw_dir / "onc_basic_ehr_by_state.csv")
        ehr_lookup = extract_state_metric(
            ehr_rows,
            ["state", "region", "region_name", "state_name"],
            ["basic_ehr", "basic", "ehr", "adoption", "percent", "pct"],
            ["year", "period", "reporting_year"],
        )

    if legacy_interop:
        interop_lookup = extract_state_metric(
            legacy_interop,
            ["state"],
            ["api_use_pct", "tefca_ready_pct", "hie_exchange_pct"],
            ["reporting_year", "year", "period"],
        )
    elif api_erx:
        interop_lookup = extract_erx_metric(api_erx)
    else:
        erx_rows = read_csv(raw_dir / "onc_surescripts_erx_state.csv")
        interop_lookup = extract_erx_metric(erx_rows)

    output: List[Dict[str, object]] = []
    all_states = sorted(set(ehr_lookup) | set(interop_lookup) | set(hie_lookup) | set(patient_access_lookup))
    for state in all_states:
        ehr_value = ehr_lookup.get(state, {}).get("value")
        ehr_year = ehr_lookup.get(state, {}).get("year", "")
        interop_value = interop_lookup.get(state, {}).get("value")
        interop_year = interop_lookup.get(state, {}).get("year", "")
        hie_value = hie_lookup.get(state, {}).get("value")
        patient_access_value = patient_access_lookup.get(state, {}).get("value")
        readiness = mean([v for v in [ehr_value, hie_value, interop_value, patient_access_value] if v is not None] or [None])

        output.append(
            {
                "state": state,
                "reporting_year": ehr_year or interop_year or "",
                "ehr_adoption_pct": ehr_value,
                "hie_exchange_pct": hie_value,
                "patient_access_pct": patient_access_value,
                "tefca_ready_pct": None,
                "api_use_pct": interop_value,
                "readiness_score": round(readiness, 1) if readiness is not None else "",
            }
        )

    fieldnames = [
        "state",
        "reporting_year",
        "ehr_adoption_pct",
        "hie_exchange_pct",
        "patient_access_pct",
        "tefca_ready_pct",
        "api_use_pct",
        "readiness_score",
    ]
    write_csv(out_dir / "onc_state.csv", output, fieldnames)


def process_ruca(raw_dir: Path, out_dir: Path) -> None:
    county_rows = read_csv(raw_dir / "ruca_by_county.csv")
    if county_rows:
        totals: Dict[str, Dict[str, int]] = {}
        for row in county_rows:
            state = normalize_state(row.get("state", ""))
            if not state:
                continue
            population = parse_int(row.get("population")) or 0
            rural_flag = parse_int(row.get("rural_flag")) or 0
            totals.setdefault(state, {"total": 0, "rural": 0})
            totals[state]["total"] += population
            if rural_flag == 1:
                totals[state]["rural"] += population
        output: List[Dict[str, object]] = []
        for state, agg in totals.items():
            total = agg["total"]
            rural = agg["rural"]
            rural_pct = round((rural / total) * 100, 1) if total else ""
            urban_pct = round(100 - rural_pct, 1) if rural_pct != "" else ""
            output.append(
                {
                    "state": state,
                    "rural_population": rural,
                    "total_population": total,
                    "rural_pct": rural_pct,
                    "urban_pct": urban_pct,
                }
            )
        fieldnames = ["state", "rural_population", "total_population", "rural_pct", "urban_pct"]
        write_csv(out_dir / "ruca_state.csv", output, fieldnames)
        return

    rows = read_csv(raw_dir / "ruca_zip_2020.csv")
    if not rows:
        print("RUCA data not found; skipping rural/urban processing.")
        return

    headers = list(rows[0].keys())
    state_col = pick_column(headers, ["state", "state_abbr", "state_code", "state_name"])
    ruca_col = pick_column(headers, ["ruca1", "ruca", "primary", "ruca_code"])
    pop_col = pick_column(headers, ["pop", "population"])

    if not state_col or not ruca_col:
        print("RUCA file missing state or RUCA code columns; skipping.")
        return

    totals: Dict[str, Dict[str, float]] = {}
    for row in rows:
        state = normalize_state(str(row.get(state_col, "")))
        if not state:
            continue
        ruca_value = parse_float(row.get(ruca_col))
        if ruca_value is None:
            continue
        weight = parse_float(row.get(pop_col)) if pop_col else 1
        if weight is None:
            weight = 1
        totals.setdefault(state, {"total": 0.0, "rural": 0.0})
        totals[state]["total"] += weight
        if ruca_value >= 4:
            totals[state]["rural"] += weight

    output = []
    for state, agg in totals.items():
        total = agg["total"]
        rural = agg["rural"]
        rural_pct = round((rural / total) * 100, 1) if total else ""
        urban_pct = round(100 - rural_pct, 1) if rural_pct != "" else ""
        output.append(
            {
                "state": state,
                "rural_population": round(rural),
                "total_population": round(total),
                "rural_pct": rural_pct,
                "urban_pct": urban_pct,
            }
        )

    fieldnames = ["state", "rural_population", "total_population", "rural_pct", "urban_pct"]
    write_csv(out_dir / "ruca_state.csv", output, fieldnames)


def aggregate_enrollment(zip_path: Path, label: str) -> Tuple[Dict[str, int], Dict[Tuple[str, str], int], Optional[int]]:
    if not zip_path.exists():
        return {}, {}, None

    member = select_zip_member(zip_path, ["full", "enrollment", label.lower()], [".csv", ".txt"])
    if not member:
        print(f"No CSV found in {zip_path}")
        return {}, {}, None

    rows = read_csv_from_zip(zip_path, member)
    if not rows:
        return {}, {}, None

    headers = list(rows[0].keys())
    state_col = pick_column(headers, ["state", "state_code", "state_abbr"])
    contract_col = pick_column(headers, ["contract", "contract_number", "contract_id", "contract number"])
    enrollment_col = pick_column(headers, ["enrollment", "enrollees", "enrolled", "enroll"])
    year_col = pick_column(headers, ["year", "contract_year", "reporting_year", "year_month"])

    if not state_col or not enrollment_col:
        print(f"Missing state or enrollment column in {zip_path}")
        return {}, {}, None

    totals: Dict[str, int] = {}
    contract_totals: Dict[Tuple[str, str], int] = {}
    year_value: Optional[int] = None

    for row in rows:
        state = normalize_state(str(row.get(state_col, "")))
        if not state:
            continue
        enrollment = parse_int(row.get(enrollment_col)) or 0
        totals[state] = totals.get(state, 0) + enrollment

        if contract_col:
            contract = str(row.get(contract_col, "")).strip()
            if contract:
                key = (state, contract)
                contract_totals[key] = contract_totals.get(key, 0) + enrollment

        if year_value is None and year_col:
            year_value = parse_int(row.get(year_col))

    if year_value is None:
        year_value = infer_year(zip_path.name)

    return totals, contract_totals, year_value


def process_cms_enrollment(raw_dir: Path, out_dir: Path) -> None:
    legacy = read_csv(raw_dir / "cms_enrollment.csv")
    if legacy:
        output: List[Dict[str, object]] = []
        for row in legacy:
            state = normalize_state(row.get("state", ""))
            if not state:
                continue
            ma_enrollment = parse_int(row.get("ma_enrollment"))
            partd_enrollment = parse_int(row.get("partd_enrollment"))
            mapd_enrollment = parse_int(row.get("mapd_enrollment"))
            pdp_enrollment = parse_int(row.get("pdp_enrollment"))
            mapd_share = None
            pdp_share = None
            if mapd_enrollment is not None and pdp_enrollment is not None:
                total = mapd_enrollment + pdp_enrollment
                if total:
                    mapd_share = round((mapd_enrollment / total) * 100, 1)
                    pdp_share = round((pdp_enrollment / total) * 100, 1)
            output.append(
                {
                    "state": state,
                    "reporting_year": row.get("reporting_year", ""),
                    "ma_enrollment": ma_enrollment,
                    "partd_enrollment": partd_enrollment,
                    "mapd_enrollment": mapd_enrollment,
                    "pdp_enrollment": pdp_enrollment,
                    "mapd_share_pct": mapd_share,
                    "pdp_share_pct": pdp_share,
                }
            )

        fieldnames = [
            "state",
            "reporting_year",
            "ma_enrollment",
            "partd_enrollment",
            "mapd_enrollment",
            "pdp_enrollment",
            "mapd_share_pct",
            "pdp_share_pct",
        ]
        write_csv(out_dir / "cms_enrollment_state.csv", output, fieldnames)
        return

    def pick_latest(prefix: str, fallback: str) -> Path:
        matches = sorted(raw_dir.glob(f"{prefix}*.zip"))
        return matches[-1] if matches else raw_dir / fallback

    ma_zip = pick_latest("cms_ma_enrollment_scc_", "cms_ma_enrollment_scc_2025_12.zip")
    pdp_zip = pick_latest("cms_pdp_enrollment_scc_", "cms_pdp_enrollment_scc_2025_12.zip")

    ma_totals, ma_contracts, ma_year = aggregate_enrollment(ma_zip, "ma")
    pdp_totals, pdp_contracts, pdp_year = aggregate_enrollment(pdp_zip, "pdp")

    if not ma_totals and not pdp_totals:
        print("CMS enrollment files not found; skipping enrollment processing.")
        return

    output: List[Dict[str, object]] = []
    all_states = sorted(set(ma_totals) | set(pdp_totals))
    reporting_year = ma_year or pdp_year or ""

    for state in all_states:
        ma_enrollment = ma_totals.get(state)
        pdp_enrollment = pdp_totals.get(state)
        mapd_enrollment = ma_enrollment
        partd_enrollment = (ma_enrollment or 0) + (pdp_enrollment or 0) if ma_enrollment or pdp_enrollment else None
        mapd_share = None
        pdp_share = None
        if mapd_enrollment is not None and pdp_enrollment is not None:
            total = mapd_enrollment + pdp_enrollment
            if total:
                mapd_share = round((mapd_enrollment / total) * 100, 1)
                pdp_share = round((pdp_enrollment / total) * 100, 1)
        output.append(
            {
                "state": state,
                "reporting_year": reporting_year,
                "ma_enrollment": ma_enrollment,
                "partd_enrollment": partd_enrollment,
                "mapd_enrollment": mapd_enrollment,
                "pdp_enrollment": pdp_enrollment,
                "mapd_share_pct": mapd_share,
                "pdp_share_pct": pdp_share,
            }
        )

    fieldnames = [
        "state",
        "reporting_year",
        "ma_enrollment",
        "partd_enrollment",
        "mapd_enrollment",
        "pdp_enrollment",
        "mapd_share_pct",
        "pdp_share_pct",
    ]
    write_csv(out_dir / "cms_enrollment_state.csv", output, fieldnames)

    contract_rows: List[Dict[str, object]] = []
    for (state, contract), enrollment in ma_contracts.items():
        contract_rows.append(
            {
                "state": state,
                "contract_id": contract,
                "plan_type": "MA",
                "enrollment": enrollment,
                "reporting_year": reporting_year,
            }
        )
    for (state, contract), enrollment in pdp_contracts.items():
        contract_rows.append(
            {
                "state": state,
                "contract_id": contract,
                "plan_type": "PDP",
                "enrollment": enrollment,
                "reporting_year": reporting_year,
            }
        )

    if contract_rows:
        write_csv(
            out_dir / "cms_contract_state_enrollment.csv",
            contract_rows,
            ["state", "contract_id", "plan_type", "enrollment", "reporting_year"],
        )


def classify_plan(org_value: Optional[str], partd_value: Optional[str]) -> Optional[str]:
    text = str(org_value or "").strip().upper()
    if text:
        if "PDP" in text or "PRESCRIPTION DRUG" in text:
            return "PDP"
        if "MA-PD" in text or "MAPD" in text or "MA PD" in text:
            return "MAPD"
        if "MA-ONLY" in text or "MA ONLY" in text or "MAONLY" in text:
            return "MA_ONLY"
        if "MEDICARE ADVANTAGE" in text and "PART D" in text:
            return "MAPD"
        if "MEDICARE ADVANTAGE" in text:
            return "MA_UNKNOWN"

    if partd_value is not None:
        flag = str(partd_value).strip().upper()
        if flag in {"Y", "YES", "1", "TRUE"}:
            return "MAPD"
        if flag in {"N", "NO", "0", "FALSE"}:
            return "MA_ONLY"

    if "MA" in text:
        return "MA_UNKNOWN"

    return None


def pick_cpsc_zip(raw_dir: Path) -> Path:
    prefixes = ["cms_enrollment_cpsc_", "monthly-enrollment-cpsc-"]
    for prefix in prefixes:
        matches = sorted(raw_dir.glob(f"{prefix}*.zip"))
        if matches:
            return matches[-1]
    return raw_dir / "cms_enrollment_cpsc_2025_12.zip"


def process_cms_plan_mix(raw_dir: Path, out_dir: Path) -> None:
    cpsc_zip = pick_cpsc_zip(raw_dir)
    if cpsc_zip.exists():
        member = select_zip_member(cpsc_zip, ["cpsc", "enrollment", "monthly"], [".csv", ".txt"])
        if not member:
            print(f"No CPSC CSV found in {cpsc_zip}")
            return
        rows = read_csv_from_zip(cpsc_zip, member)
        if not rows:
            print("CPSC file empty; skipping plan mix.")
            return

        headers = list(rows[0].keys())
        state_col = pick_column(headers, ["state", "state_code", "state_abbr"])
        enrollment_col = pick_column(headers, ["enrollment", "enrollees", "enrolled", "enroll"])
        org_col = pick_column(headers, [
            "organization type",
            "organization_type",
            "org type",
            "org_type",
            "plan_type",
            "plan type",
            "contract type",
            "contract_type",
        ])
        partd_col = pick_column(headers, [
            "part d",
            "partd",
            "part_d",
            "drug",
            "rx",
            "pd",
        ])
        year_col = pick_column(headers, ["year", "contract_year", "reporting_year", "year_month"])

        if not state_col or not enrollment_col:
            print("CPSC file missing state or enrollment columns; skipping plan mix.")
            return

        totals: Dict[str, Dict[str, int]] = {}
        year_value: Optional[int] = None

        for row in rows:
            state = normalize_state(str(row.get(state_col, "")))
            if not state:
                continue
            enrollment = parse_int(row.get(enrollment_col)) or 0
            org_value = row.get(org_col) if org_col else ""
            partd_value = row.get(partd_col) if partd_col else None
            classification = classify_plan(str(org_value), str(partd_value) if partd_value is not None else None)
            totals.setdefault(state, {"mapd": 0, "ma_only": 0, "pdp": 0, "ma_unknown": 0})
            if classification == "MAPD":
                totals[state]["mapd"] += enrollment
            elif classification == "MA_ONLY":
                totals[state]["ma_only"] += enrollment
            elif classification == "PDP":
                totals[state]["pdp"] += enrollment
            elif classification == "MA_UNKNOWN":
                totals[state]["ma_unknown"] += enrollment

            if year_value is None and year_col:
                year_value = parse_int(row.get(year_col))

        if year_value is None:
            year_value = infer_year(cpsc_zip.name)

        output: List[Dict[str, object]] = []
        for state, counts in totals.items():
            mapd = counts["mapd"]
            ma_only = counts["ma_only"]
            pdp = counts["pdp"]
            ma_unknown = counts["ma_unknown"]
            ma_total = mapd + ma_only + ma_unknown

            split_method = "mapd_ma_only" if (mapd > 0 or ma_only > 0) else "ma_vs_pdp"
            if split_method == "mapd_ma_only":
                total = mapd + ma_only + pdp
                mapd_share = round((mapd / total) * 100, 1) if total else None
                ma_only_share = round((ma_only / total) * 100, 1) if total else None
                pdp_share = round((pdp / total) * 100, 1) if total else None
            else:
                mapd = ma_total if ma_total else None
                ma_only = None
                total = (mapd or 0) + pdp
                mapd_share = round(((mapd or 0) / total) * 100, 1) if total else None
                ma_only_share = None
                pdp_share = round((pdp / total) * 100, 1) if total else None

            output.append(
                {
                    "state": state,
                    "reporting_year": year_value or "",
                    "mapd_enrollment": mapd,
                    "ma_only_enrollment": ma_only,
                    "ma_total_enrollment": ma_total,
                    "pdp_enrollment": pdp,
                    "mapd_share_pct": mapd_share,
                    "ma_only_share_pct": ma_only_share,
                    "pdp_share_pct": pdp_share,
                    "split_method": split_method,
                }
            )

        fieldnames = [
            "state",
            "reporting_year",
            "mapd_enrollment",
            "ma_only_enrollment",
            "ma_total_enrollment",
            "pdp_enrollment",
            "mapd_share_pct",
            "ma_only_share_pct",
            "pdp_share_pct",
            "split_method",
        ]
        write_csv(out_dir / "cms_plan_mix_state.csv", output, fieldnames)
        return

    # Fallback to existing enrollment state totals
    enrollment_rows = read_csv(out_dir / "cms_enrollment_state.csv")
    if not enrollment_rows:
        print("No enrollment data for plan mix; skipping.")
        return

    output: List[Dict[str, object]] = []
    for row in enrollment_rows:
        state = normalize_state(row.get("state", ""))
        if not state:
            continue
        mapd_enrollment = parse_int(row.get("mapd_enrollment"))
        pdp_enrollment = parse_int(row.get("pdp_enrollment"))
        total = (mapd_enrollment or 0) + (pdp_enrollment or 0)
        mapd_share = round(((mapd_enrollment or 0) / total) * 100, 1) if total else None
        pdp_share = round(((pdp_enrollment or 0) / total) * 100, 1) if total else None
        output.append(
            {
                "state": state,
                "reporting_year": row.get("reporting_year", ""),
                "mapd_enrollment": mapd_enrollment,
                "ma_only_enrollment": None,
                "ma_total_enrollment": mapd_enrollment,
                "pdp_enrollment": pdp_enrollment,
                "mapd_share_pct": mapd_share,
                "ma_only_share_pct": None,
                "pdp_share_pct": pdp_share,
                "split_method": "ma_vs_pdp",
            }
        )

    fieldnames = [
        "state",
        "reporting_year",
        "mapd_enrollment",
        "ma_only_enrollment",
        "ma_total_enrollment",
        "pdp_enrollment",
        "mapd_share_pct",
        "ma_only_share_pct",
        "pdp_share_pct",
        "split_method",
    ]
    write_csv(out_dir / "cms_plan_mix_state.csv", output, fieldnames)


def candidate_star_members(zip_path: Path) -> List[str]:
    if not zip_path.exists():
        return []
    with ZipFile(zip_path, "r") as handle:
        members = [info for info in handle.infolist() if info.filename.lower().endswith((".xlsx", ".xls", ".csv"))]
    scored = []
    keywords = ["star", "rating", "overall", "summary", "contract"]
    for info in members:
        name = info.filename.lower()
        score = sum(1 for keyword in keywords if keyword in name)
        scored.append((score, info.file_size, info.filename))
    scored.sort(reverse=True)
    return [entry[2] for entry in scored]


def parse_star_ratings(zip_path: Path) -> Tuple[Dict[str, float], Optional[int]]:
    if not zip_path.exists():
        return {}, None

    star_year = infer_year(zip_path.name)
    for member in candidate_star_members(zip_path):
        if member.lower().endswith(".csv"):
            rows = read_csv_from_zip(zip_path, member)
        else:
            rows = read_excel_from_zip(zip_path, member, max_rows=1000)

        if not rows:
            continue

        headers = list(rows[0].keys())
        contract_col = pick_column(headers, ["contract", "contract_id", "contract number"])
        rating_col = pick_column(
            headers,
            [
                "overall",
                "overall_rating",
                "overall_star",
                "overall_star_rating",
                "star rating",
                "summary rating",
                "rating",
            ],
        )
        if not contract_col or not rating_col:
            continue

        ratings: Dict[str, float] = {}
        for row in rows:
            contract = str(row.get(contract_col, "")).strip()
            if not contract:
                continue
            rating = parse_float(row.get(rating_col))
            if rating is None:
                continue
            ratings[contract] = rating

        if ratings:
            return ratings, infer_year(member) or star_year

    print("Unable to identify contract-level star ratings in ZIP.")
    return {}, star_year


def process_cms_stars(raw_dir: Path, out_dir: Path) -> None:
    legacy = read_csv(raw_dir / "cms_stars.csv")
    if legacy:
        output: List[Dict[str, object]] = []
        for row in legacy:
            state = normalize_state(row.get("state", ""))
            if not state:
                continue
            output.append(
                {
                    "state": state,
                    "reporting_year": row.get("reporting_year", ""),
                    "avg_star": parse_float(row.get("avg_star")),
                    "volatility_index": parse_float(row.get("volatility_index")),
                    "churn_pct": parse_float(row.get("churn_pct")),
                }
            )

        fieldnames = ["state", "reporting_year", "avg_star", "volatility_index", "churn_pct"]
        write_csv(out_dir / "cms_stars_state.csv", output, fieldnames)
        return

    matches = sorted(raw_dir.glob("cms_star_ratings_data_tables_*.zip"))
    star_zip = matches[-1] if matches else raw_dir / "cms_star_ratings_data_tables_2026.zip"
    ratings, star_year = parse_star_ratings(star_zip)
    if not ratings:
        print("Star ratings data not found; skipping CMS stars processing.")
        return

    enrollment_rows = read_csv(out_dir / "cms_contract_state_enrollment.csv")
    if not enrollment_rows:
        print("Contract enrollment file missing; unable to weight star ratings by state.")
        return

    state_ratings: Dict[str, List[Tuple[float, int]]] = {}
    reporting_year = str(star_year) if star_year else ""
    for row in enrollment_rows:
        state = normalize_state(row.get("state", ""))
        contract = str(row.get("contract_id", "")).strip()
        enrollment = parse_int(row.get("enrollment")) or 0
        rating = ratings.get(contract)
        if not state or rating is None or enrollment == 0:
            continue
        state_ratings.setdefault(state, []).append((rating, enrollment))

    output: List[Dict[str, object]] = []
    for state, entries in state_ratings.items():
        total_enrollment = sum(weight for _, weight in entries)
        weighted_avg = None
        if total_enrollment:
            weighted_avg = round(sum(rating * weight for rating, weight in entries) / total_enrollment, 2)
        ratings_only = [rating for rating, _ in entries]
        volatility = round(pstdev(ratings_only), 3) if len(ratings_only) > 1 else None
        output.append(
            {
                "state": state,
                "reporting_year": reporting_year,
                "avg_star": weighted_avg,
                "volatility_index": volatility,
                "churn_pct": None,
            }
        )

    fieldnames = ["state", "reporting_year", "avg_star", "volatility_index", "churn_pct"]
    write_csv(out_dir / "cms_stars_state.csv", output, fieldnames)


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize raw datasets into state-level tables.")
    parser.add_argument("--raw", default="data/raw")
    parser.add_argument("--processed", default="data/processed")
    parser.add_argument("--use-samples", action="store_true", help="If data/raw is empty, use data/samples/raw instead.")
    args = parser.parse_args()

    raw_dir = Path(args.raw)
    processed_dir = Path(args.processed)

    if args.use_samples:
        has_files = False
        if raw_dir.exists():
            has_files = any(raw_dir.glob("*.csv")) or any(raw_dir.glob("*.zip")) or any(raw_dir.glob("*.xlsx"))
        if not raw_dir.exists() or not has_files:
            raw_dir = Path("data/samples/raw")

    processed_dir.mkdir(parents=True, exist_ok=True)

    process_onc(raw_dir, processed_dir)
    process_ruca(raw_dir, processed_dir)
    process_cms_enrollment(raw_dir, processed_dir)
    process_cms_plan_mix(raw_dir, processed_dir)
    process_cms_stars(raw_dir, processed_dir)

    print(f"processed data written to {processed_dir}")


if __name__ == "__main__":
    main()
