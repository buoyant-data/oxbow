#!/usr/bin/env bash
#
# Run the oxbow end-to-end acceptance tests against LocalStack.
#
#   podman compose up -d        # start LocalStack (see compose.yml)
#   ./acceptance/run.sh [pytest args...]
#
# Lambda artifacts are taken from target/lambda/x86-64 and are built with the
# `release` cargo profile when they are missing. The `ci` profile produces zips
# over LocalStack's 50 MB limit, so it cannot be used here. Set REBUILD=1 to force a rebuild.
# Set BUILD_PROFILE to use a different cargo profile.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

VENV="${REPO_ROOT}/venv"
BUILD_PROFILE="${BUILD_PROFILE:-release}"
LAMBDA_DIR="target/lambda/x86-64"
OXBOW_ZIP="${LAMBDA_DIR}/oxbow-lambda/bootstrap.zip"
SQS_INGEST_ZIP="${LAMBDA_DIR}/sqs-ingest/bootstrap.zip"

if [ -f "${HOME}/.cargo/env" ]; then
        # shellcheck disable=SC1091
        . "${HOME}/.cargo/env"
fi

if [ ! -x "${VENV}/bin/python" ]; then
        echo ">> ${VENV} is missing; create it with: python3 -m venv venv && venv/bin/pip install boto3 pytest" >&2
        exit 1
fi
# shellcheck disable=SC1091
. "${VENV}/bin/activate"

if ! python -c "import boto3, pytest" 2>/dev/null; then
        echo ">> Installing acceptance test dependencies into ${VENV}"
        pip install --quiet boto3 pytest
fi

build_lambda() {
        local package="$1"
        echo ">> Building ${package} (profile: ${BUILD_PROFILE})"
        cargo lambda build \
                --package "${package}" \
                --profile "${BUILD_PROFILE}" \
                --output-format zip \
                --x86-64 \
                --lambda-dir "${LAMBDA_DIR}"
}

if [ -n "${REBUILD:-}" ] || [ ! -f "${OXBOW_ZIP}" ]; then
        build_lambda oxbow-lambda
fi
if [ -n "${REBUILD:-}" ] || [ ! -f "${SQS_INGEST_ZIP}" ]; then
        build_lambda sqs-ingest
fi

exec python -m pytest acceptance "$@"
