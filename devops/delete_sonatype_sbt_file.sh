#!/usr/bin/env bash

function log() {

    echo "$(date +"%Y-%m-%d %H:%M:%S")" "[$1]" "$2"
}


DIRECTORY="$HOME/.sbt/1.0"
if [[ -d $DIRECTORY ]];
then
  SONATYPE_FILE=$DIRECTORY/sonatype.sbt
  if [[ -f $SONATYPE_FILE ]];
  then
    rm "$SONATYPE_FILE" && log "INFO" "Successfully removed Sonatype .sbt file"
  else
    log "WARNING" "File $SONATYPE_FILE not found"
  fi
else
  log "WARNING" "Directory $DIRECTORY not found"
fi