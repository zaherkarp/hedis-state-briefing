#!/bin/sh
set -e

if [ ! -f web/data/index.json ]; then
  python3 scripts/process.py --use-samples
  python3 scripts/build.py
fi

exec "$@"
