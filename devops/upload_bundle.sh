#!/usr/bin/env bash

function log() {

    echo "$(date +"%Y-%m-%d %H:%m:%S")" "[$1]" "$2"
}

bundle_opt_short="b"
bundle_opt_long="bundle"
username_opt_short="u"
username_opt_long="username"
password_opt_short="p"
password_opt_long="password"

while [[ $# -gt 0 ]]; do
  case $1 in
    "-$bundle_opt_short"|"--$bundle_opt_long")
      bundle_path="$2"
      shift # past argument
      shift # past value
      ;;
    "-$username_opt_short"|"--$username_opt_long")
      token_username="$2"
      shift # past argument
      shift # past value
      ;;
    "-$password_opt_short"|"--$password_opt_long")
      token_password="$2"
      shift # past argument
      shift # past value
      ;;
    *)
      shift # past argument
      ;;
  esac
done

if [[ -n $bundle_path && -n $token_username && -n $token_password ]];
then

  # check if bundle exists
  if [[ -f $bundle_path ]];
  then
    bearer_token=$("$token_username":"$token_password" | base64)
  else
    log "ERROR" "Bundle $bundle_path does not exist. Please provide the path to an existing bundle"
    return 1
  fi
else

  # log the error and return 1
  log "ERROR" "Missing 1 or more required parameters. Usage: upload_bundle.sh [PARAMS]. Required parameters

  -$bundle_opt_short,--$bundle_opt_long         Path of the bundle to upload at Central Sonatype
  -$username_opt_short,--$username_opt_long       Token username (created at Central Sonatype)
  -$password_opt_short,--$password_opt_long       Token password (created at Central Sonatype)
  "

  return 1
fi