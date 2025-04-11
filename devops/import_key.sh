#!/usr/bin/env bash

function log() {

    echo "$(date +"%Y-%m-%d %H:%m:%S")" "[$1]" "$2"
}

echo "$GPG_PRIVATE_KEY" | gpg --batch --yes --import && log "INFO" "Successfully imported key"