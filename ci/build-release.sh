#!/bin/sh

if [ -f "${HOME}/.cargo/env" ]; then
        source "${HOME}/.cargo/env"
fi;

exec cargo lambda build --release --output-format zip
