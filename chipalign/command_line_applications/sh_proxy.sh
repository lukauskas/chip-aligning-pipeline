#!/usr/bin/env bash
# See https://github.com/amoffat/sh/issues/392#issuecomment-327759713
set -e
TOTAL_LEN=${#@}
EXP_LEN=`expr ${TOTAL_LEN} - 2`
"${@:1:${EXP_LEN}}">"${@:${TOTAL_LEN}}"