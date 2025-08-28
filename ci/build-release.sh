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

#PLATFORMS=("x86-64" "arm64")
PLATFORMS=("x86-64")

for d in $(ls ./lambdas); do
        if [ ! -d "./lambdas/${d}" ]; then
                echo ">> ${d} is not a directory? por que?";
                exit 1
        fi;
        echo ">> Building release for ${d}"

        # The package name for the oxbow lambda is not just "oxbow" because it would collide with the crate named "oxbow"
        if [ "${d}" = "oxbow" ]; then
                d="oxbow-lambda";
        fi

        for p in ${PLATFORMS[@]}; do
                echo ">> ${p}"
                time cargo lambda build --package ${d} --release --output-format zip --${p} --lambda-dir target/lambda/${p}
                mv target/lambda/${p}/${d}/bootstrap.zip target/lambda/${d}-${p}.zip
        done;
done;
