#!/bin/sh 

set -xe

cargo fmt --check

exec cargo build
