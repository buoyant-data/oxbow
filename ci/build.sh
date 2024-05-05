#!/bin/sh 

if [ -f "${HOME}/.cargo/env" ]; then
        source "${HOME}/.cargo/env"
fi;

set -xe

cargo fmt --check

exec cargo build
