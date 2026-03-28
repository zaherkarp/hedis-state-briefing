# HEDIS State Briefing Wall

## Quick Start

```bash
pip install -r requirements.txt -r requirements-dev.txt
python scripts/run_pipeline.py --use-samples
python -m pytest tests/ -v
```

## Project Structure

- `scripts/` - Python data pipeline (fetch → process → build)
- `web/` - Static single-page briefing wall (vanilla JS/CSS)
- `data/` - Raw inputs, processed CSVs, generated JSON artifacts
- `tests/` - pytest test suite
- `reports/` - Coverage and QA output

## Key Commands

- **Run pipeline with sample data**: `python scripts/run_pipeline.py --use-samples`
- **Run tests**: `python -m pytest tests/ -v`
- **Serve locally**: `python -m http.server 8000 --directory web`
- **Docker**: `docker build -t hedis-wall . && docker run --rm -p 8000:8000 hedis-wall`

## Pipeline Stages

1. `fetch.py` - Downloads raw data from URLs in `data/config/sources.yml`
2. `process.py` - Normalizes raw CSV → state-level tables in `data/processed/`
3. `build.py` - Generates per-state JSON briefings in `data/states/` and mirrors to `web/data/`

## Architecture Notes

- The web UI is a static SPA with no build step; edit `web/` files directly
- All data sources are disabled by default in `sources.yml`; use `--allow-disabled` to fetch
- Sample data covers CA, FL, IA for local development
- State JSON payloads are the contract between pipeline and UI
