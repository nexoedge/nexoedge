#!/bin/sh

if [ $# -lt 1 ]; then
  echo "Usage: ${0} <webdis config file>"
  exit 1
fi

if [ ! -z `echo "${REDIS_HOST}" | grep ^\$\{.*\}$` ]; then
  l=`echo ${REDIS_HOST} | sed 's/\${\(.*\)}/\1/'`
  REDIS_HOST=$(eval "echo \$$l")
elif [ ! -z `echo "${REDIS_HOST}" | grep ^\$.*$` ]; then
  l=`echo ${REDIS_HOST} | sed 's/\$\(.*\)/\1/'`
  REDIS_HOST=$(eval "echo \$$l")
fi

if [ ! -z `echo "${REDIS_PORT}" | grep ^\$\{.*\}$` ]; then
  l=`echo ${REDIS_PORT} | sed 's/\${\(.*\)}/\1/'`
  REDIS_PORT=$(eval "echo \$$l")
elif [ ! -z `echo "${REDIS_PORT}" | grep ^\$.*$` ]; then
  l=`echo ${REDIS_PORT} | sed 's/\$\(.*\)/\1/'`
  REDIS_PORT=$(eval "echo \$$l")
fi

echo "StatsDB at ${REDIS_HOST}:${REDIS_PORT}"

sed -i "s/\(\"redis_host\":\t\"\).*\(\",\)/\1${REDIS_HOST}\2/" ${1}
sed -i "s/\(\"redis_port\":\t\).*\(,\)/\1${REDIS_PORT}\2/" ${1}

