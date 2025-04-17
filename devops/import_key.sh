#!/usr/bin/env bash

function log() {

    echo "$(date +"%Y-%m-%d %H:%M:%S")" "[$1]" "$2"
}

echo "$1" | gpg --batch --yes --quiet --import && log "INFO" "Successfully imported key"