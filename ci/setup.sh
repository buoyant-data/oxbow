#!/usr/bin/env bash

if [ -f "${HOME}/.cargo/env" ]; then
        . "${HOME}/.cargo/env"
fi;

which cargo-lambda

if [ $? -ne 0 ]; then
        cargo install cargo-lambda
fi;

# Prefer virtualenv when it is installed, otherwise use the stdlib venv module
# (hosted runners such as GitHub Actions do not ship the virtualenv command)
if command -v virtualenv >/dev/null 2>&1; then
        virtualenv venv
else
        python3 -m venv venv
fi;

source venv/bin/activate
pip install ziglang
pip install boto3 pytest
