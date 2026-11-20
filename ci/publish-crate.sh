#!/usr/bin/env bash
#
# Publish a single crate to crates.io. If the exact version is already
# published, that is treated as a non-fatal no-op so re-runs of the release
# workflow succeed. Any other failure still fails the script.
#
# Usage: ci/publish-crate.sh <package-name>

set -eauo pipefail

if [ "$#" -ne 1 ]; then
        echo "usage: $0 <package-name>" >&2
        exit 2
fi

package="$1"
log="$(mktemp)"
trap 'rm -f "${log}"' EXIT

set +e
cargo publish --package "${package}" 2>&1 | tee "${log}"
status="${PIPESTATUS[0]}"
set -e

if [ "${status}" -eq 0 ]; then
        exit 0
fi

if grep -q "already exists on crates.io index" "${log}"; then
        echo "::warning::${package} is already published at this version; skipping"
        exit 0
fi

exit "${status}"
