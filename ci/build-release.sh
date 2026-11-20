#!/usr/bin/env bash

set -eau pipefail

if [ -f "${HOME}/.cargo/env" ]; then
        . "${HOME}/.cargo/env"
fi;

if [ ! -d venv ]; then
        python3 -m venv venv
        . venv/bin/activate
        pip3 install cargo-lambda ziglang
fi;

. venv/bin/activate

# Space-separated list of platforms to build, overridable at runtime, e.g.
# PLATFORMS="arm64" ./ci/build-release.sh
PLATFORMS="${PLATFORMS:-x86-64 arm64}"
read -ra PLATFORM_LIST <<< "${PLATFORMS}"

# Cargo build profile for `cargo lambda build --profile`. Must match a
# [profile.<name>] section in the root Cargo.toml (release, dist, ciexist).
BUILD_PROFILE="${BUILD_PROFILE:-release}"

for d in $(ls ./lambdas); do
        if [ ! -d "./lambdas/${d}" ]; then
                echo ">> ${d} is not a directory? por que?";
        else
                echo ">> Building release for ${d}"

                # The package name for the oxbow lambda is not just "oxbow" because it would collide with the crate named "oxbow"
                if [ "${d}" = "oxbow" ]; then
                        d="oxbow-lambda";
                fi

                for p in ${PLATFORM_LIST[@]}; do
                        echo ">> ${p}"
                        time cargo lambda build --package ${d} --profile ${BUILD_PROFILE}  --output-format zip --${p} --lambda-dir target/lambda/${p}
                        mv target/lambda/${p}/${d}/bootstrap.zip target/lambda/${d}-${p}.zip
                done;
        fi;
done;
