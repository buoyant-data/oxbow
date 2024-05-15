#!/usr/bin/env bash

function rename_artifact() {
        mkdir -p ./target/artifacts
        NEW_FILE=$(echo "${1}" | sed 's!./target/lambda/!!' | sed 's/\//\-/g')
        mv "${1}" "./target/artifacts/${NEW_FILE}"
}

for f in $(find ./target/lambda -iname "bootstrap.zip" -print ); do
        rename_artifact "${f}"
done;
