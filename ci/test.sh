#!/bin/sh 
if [ -f "${HOME}/.cargo/env" ]; then
        . "${HOME}/.cargo/env"
fi;

SUBCOMMAND="test"

set -e
which cargo-nextest
if [ $? -eq 0 ]; then
        SUBCOMMAND="nextest r --profile ci"
fi;
set +e

exec cargo ${SUBCOMMAND}
