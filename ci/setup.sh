#!/usr/bin/env bash

if [ -f "${HOME}/.cargo/env" ]; then
        . "${HOME}/.cargo/env"
fi;

which cargo-lambda

if [ $? -ne 0 ]; then
        cargo install cargo-lambda
fi;

which virtualenv

if [ $? -ne 0 ]; then
        echo ">> Virtualenv is required in order to setup cargo-lambda here!"
        exit 1;
fi;

virtualenv venv

source venv/bin/activate
pip install ziglang
