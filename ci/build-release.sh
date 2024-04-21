#!/bin/sh

exec cargo lambda build --release --output-format zip
