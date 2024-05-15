#!/bin/sh 
if [ -f "${HOME}/.cargo/env" ]; then
        . "${HOME}/.cargo/env"
fi;


exec cargo test --verbose
