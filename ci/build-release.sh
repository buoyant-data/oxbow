#!/bin/sh

if [ -f "${HOME}/.cargo/env" ]; then
        . "${HOME}/.cargo/env"
fi;

. venv/bin/activate

exec cargo lambda build --release --output-format zip
