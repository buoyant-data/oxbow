#!/bin/sh

if [ -f "${HOME}/.cargo/env" ]; then
        . "${HOME}/.cargo/env"
fi;

. venv/bin/activate

exec cargo lambda build --compiler cross --release --output-format zip
