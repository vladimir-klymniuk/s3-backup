#! /bin/bash

# set -e command instructs bash to exit this script at first error
set -e

if [ -e ~/.bash_profile ]; then
  shopt -s expand_aliases
  source ~/.bash_profile
fi

export __ScriptFile=${0##*/}
export __ScriptName=${__ScriptFile%.sh}
export __ScriptPath=${0%/*}; __ScriptPath=${__ScriptPath%/}

ENVIRONMENT=$1
TARGETDIR=/home/adbroker/s3_backup/work

HOME=/home/adbroker
DATE=$(date +%Y-%m-%d_%H-%M-%S)
BACKUPDIR="${HOME}/s3_backup/backup"

if [[ -e $TARGETDIR ]]; then

  BACKUPDIR_OLD="${BACKUPDIR}/s3_backup.old.${DATE}"
  mkdir -p "${BACKUPDIR_OLD}"
  cp -ap "${TARGETDIR}" "${BACKUPDIR_OLD}"

fi

cp -af ${__ScriptPath}/*.py $TARGETDIR

IFS='_' read -ra DATA_CENTER <<< "$ENVIRONMENT"
cp ${__ScriptPath}/config_${DATA_CENTER[1]}.json $TARGETDIR/config.json

if [[ -e $TARGETDIR ]]; then

  BACKUPDIR_NEW="${BACKUPDIR}/s3_backup.new.${DATE}"
  mkdir -p "${BACKUPDIR_NEW}"
  cp -ap "${TARGETDIR}" "${BACKUPDIR_NEW}"
  chmod +x $TARGETDIR/*.py

fi

PID=`ps aux | grep log_downloader | grep -v grep | grep -v '/bin/sh' | awk '{print $2}'`
if [ -n "$PID" ]; then
  kill "$PID"
fi