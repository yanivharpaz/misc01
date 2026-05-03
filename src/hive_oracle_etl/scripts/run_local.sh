#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-.env}"
PYTHONPATH="${PYTHONPATH:-src}" python -m hive_oracle_etl.main --env-file "${ENV_FILE}" --print-query
