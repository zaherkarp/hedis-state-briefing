#!/bin/sh
set -e

if [ ! -f web/data/index.json ]; then
  python3 scripts/run_pipeline.py --use-samples --skip-fetch --skip-qa
fi

exec "$@"
