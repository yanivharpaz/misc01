#!/usr/bin/env bash
set -euo pipefail

JOB_NAME="hive-oracle-etl-manual-$(date +%Y%m%d%H%M%S)"
oc create job "${JOB_NAME}" --from=cronjob/hive-oracle-etl
oc logs -f "job/${JOB_NAME}"
