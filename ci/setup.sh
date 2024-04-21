#!/usr/bin/env bash

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
