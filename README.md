# Stars / HEDIS State Briefing Wall (ECDS + Pre-Season Shift)

## What this is
A narrative, single-page briefing wall that explains how Stars / HEDIS operations are shifting by state. It prioritizes ECDS readiness, pre-season timing shifts, rural vs urban constraints, MAPD vs PDP structure, and role-level disruption. It is not a BI dashboard.

## Who it is for
Operational leaders in health systems and health plans who need a clear, state-specific picture of what is changing and why it matters.

## Repo structure
- `data/config/` – pinned source URLs and role impact defaults
- `data/raw/` – downloaded raw datasets
- `data/processed/` – normalized, state-level tables
- `data/states/` – per-state JSON briefing artifacts
- `data/samples/` – lightweight sample inputs for local development
- `scripts/` – fetch, process, build, and coverage reporting
- `web/` – static wall (HTML/CSS/JS)
- `reports/` – periodic coverage reports

## Initial testing (exhaustive)
This walkthrough assumes you are starting from a clean clone with no cached outputs and
you are running commands inside the repo root.

### 1) Use OpenSSL-linked Python (recommended)
Homebrew Python is linked against OpenSSL and avoids the LibreSSL warning.
Create a local virtual environment with it:
```bash
/opt/homebrew/bin/python3 -m venv .venv
```
Activate it (recommended so `python3` points to the OpenSSL build):
```bash
source .venv/bin/activate
```

### 2) Install dependencies
Runtime deps:
```bash
python3 -m pip install -r requirements.txt
```

Dev/test deps:
```bash
python3 -m pip install -r requirements-dev.txt
```

### 3) Run the sample-data pipeline (recommended first run)
This runs fetch → process → build → coverage report → QA checks using `data/samples/raw`.
```bash
uv run python scripts/run_pipeline.py --use-samples
```

If you are not using `uv`, run the steps manually:
```bash
python3 scripts/process.py --use-samples
python3 scripts/build.py
python3 scripts/coverage_report.py --top 5
```

### 4) Validate outputs
You should now have:
- `data/processed/*.csv` (state-level tables)
- `data/states/<STATE>.json` and `data/states/index.json` (briefing artifacts)
- `web/data/<STATE>.json` and `web/data/index.json` (mirrored artifacts for the UI)
- `reports/coverage/coverage_YYYY-MM-DD.md` (coverage report)

### 5) Open the wall
Serve the `web/` directory with any static server and open `index.html`.
```bash
python3 -m http.server --directory web 8000
```
Then open `http://localhost:8000`.

### 6) Run tests
```bash
python3 -m pytest
```
Expected: 1 test passing (pipeline on sample data).

### 7) Interpret the coverage report
Coverage reports summarize:
- Top states per metric (readiness, rural %, MAPD/PDP splits, stars, volatility, enrollment).
- Missing data coverage by metric.

If the sample data is used, the report will only include the sample states (typically CA/FL/IA).
If MA-only share is missing, it will be listed for every state. This indicates the CPSC
plan-mix split did not populate `ma_only_share_pct`.

## One-command run (real data)
```bash
uv run python scripts/run_pipeline.py --allow-disabled
```

## Update data annually
1. Update pinned URLs in `data/config/sources.yml` (CMS data tables, enrollment zips, and any ONC datasets).
2. Fetch raw data:
   ```bash
   python3 scripts/fetch.py
   ```
3. Process to state-level tables:
   ```bash
   python3 scripts/process.py
   ```
4. Build the state JSON artifacts:
   ```bash
   python3 scripts/build.py
   ```

## Testing
```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest
```

## Coverage report (periodic)
After `build.py`, generate a report of top states per category and missing data coverage:
```bash
python3 scripts/coverage_report.py --top 5
```
Reports are saved to `reports/coverage/`.

## QA checks (periodic)
QA checks validate key outputs and coverage:
```bash
python3 scripts/qa_checks.py
```
Reports are saved to `reports/qa/`. To skip QA in the full pipeline, use `--skip-qa`.
 
## Troubleshooting warnings
### Homebrew shellenv warning (`/opt/homebrew/.../shellenv.sh: /bin/ps: Operation not permitted`)
This is a macOS permission warning triggered by a login shell executing Homebrew’s `shellenv`.
It does not affect the pipeline. To avoid it:
- Run commands in a non-login shell in automated contexts (for example, avoid launching shells with `-l`).
- Or remove/guard the `brew shellenv` line in your shell profile if you want to stop it globally.

### urllib3 + LibreSSL warning
If you see:
```
NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL ...'
```
This comes from system Python on macOS. The pipeline still runs, but you can silence it by:
- Pinning `urllib3<2` in `requirements.txt` and `pyproject.toml`, or
- Using a Python build linked against OpenSSL (for example, Homebrew Python).

## Docker
Build the image:
```bash
docker build -t hedis-wall .
```
Run with sample data (auto-builds the wall if `web/data` is missing):
```bash
docker run --rm -p 8000:8000 hedis-wall
```
Then open `http://localhost:8000`.

To use real data, mount a volume and run the fetch/process/build steps:
```bash
docker run --rm -it -v "$PWD":/app hedis-wall \
  python3 scripts/fetch.py --allow-disabled

docker run --rm -it -v "$PWD":/app hedis-wall \
  python3 scripts/process.py

docker run --rm -it -v "$PWD":/app hedis-wall \
  python3 scripts/build.py
```

## Source notes
- CMS Star Ratings Data Tables are zipped and update annually. The pipeline searches for a contract-level overall rating table and weights ratings by enrollment when available.
- CMS enrollment inputs are sourced from monthly MA and PDP enrollment ZIPs (state/county/contract). Update these monthly for the newest enrollment mix.
- MAPD vs MA-only splits use the CPSC monthly enrollment file when available. If that split is missing, the wall falls back to MA vs PDP shares.
- RUCA ZIP-based files are supported. If the RUCA file does not include a state column, add a ZIP-to-state crosswalk before processing.
- ONC per-state pulls (open-api.php) are configured for Wisconsin, Maryland, and New York in `sources.yml`. Enable `onc_api` to use those API calls.

## Add interview data later
The state JSON schema already includes extension points:
- `future.organizations` for rural + urban org profiles
- `future.interviews` for qualitative notes
- `future.role_risk_scores` for quantitative role impact

When you are ready, extend `scripts/build.py` to merge interview datasets into each state payload, and update the UI to render those fields.

## Design intent
- Long scroll, single-page narrative
- Sections prioritize ECDS readiness, pre-season shifts, rural/urban context, MAPD/PDP split, and role disruption
- Tables and callouts over heavy charts
- Graceful fallback when data is missing
