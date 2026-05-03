#!/usr/bin/env bash
set -euo pipefail

PROJECT="${1:?usage: scripts/oc_deploy.sh <openshift-project> <image-tag>}"
IMAGE_TAG="${2:?usage: scripts/oc_deploy.sh <openshift-project> <image-tag>}"

oc project "${PROJECT}"
oc apply -k deploy/openshift
oc set image cronjob/hive-oracle-etl hive-oracle-etl="${IMAGE_TAG}"
