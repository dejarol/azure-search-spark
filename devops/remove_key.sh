#!/usr/bin/env bash

function log() {

    echo "$(date +"%Y-%m-%d %H:%M:%S")" "[$1]" "$2"
}

KEY_ID=$(gpg --list-secret-keys --with-colons | awk -F: '/^sec/ {print $5}')
if [[ -n $KEY_ID ]];
then
    gpg --batch --yes --quiet --delete-secret-keys "$KEY_ID" && log "INFO" "Successfully removed secret key"
    gpg --batch --yes --quiet --delete-keys "$KEY_ID" && log "INFO" "Successfully removed key"
else
  log "WARNING" "No key was found"
fi