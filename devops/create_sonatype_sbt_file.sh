#!/usr/bin/env bash

function log() {

    echo "$(date +"%Y-%m-%d %H:%m:%S")" "[$1]" "$2"
}

USERNAME="$1"
PASSWORD="$2"
if [[ -z $USERNAME || -z $PASSWORD ]];
then
  log "ERROR" "One among username or password was not given. Please provide username as first parameter and password as second"
  return 1
fi

# Create directory if needed
DIRECTORY="$HOME/.sbt/1.0"
if [[ -d $DIRECTORY ]];
then
  log "INFO" "Directory $DIRECTORY already exists"
else
  mkdir -p "$DIRECTORY"
  log "INFO" "Created directory $DIRECTORY"
fi

# Create file
cat <<EOL > "$DIRECTORY/sonatype.sbt" && log "INFO" "Successfully created credentials file"
  credentials += Credentials("Sonatype Nexus Repository Manager",
          "central.sonatype.com",
          "$USERNAME",
          "$PASSWORD")
EOL

ls -l "$DIRECTORY"

