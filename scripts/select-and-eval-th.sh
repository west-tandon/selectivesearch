#!/bin/bash

#
# Arguments:
# 1) basename
# 2) qrels
# *) budget(s)
#

basename=$1
qrels=$2
shift 2

if [ -z "${basename}" ]; then echo "You have to define basename (1)."; exit 1; fi;
if [ -z "${qrels}" ]; then echo "You have to define qrels (2)."; exit 1; fi;
if [ "$#" == "0" ]; then echo "You have to define threshold(s) to test."; exit 1; fi

set -x
set -e

while (( "$#" )); do

threshold=$1

selectivesearch select-shards ${basename} --threshold ${threshold}

thresholdBase="${basename}%[${threshold}]"

selectivesearch selection2time "${thresholdBase}"
selectivesearch overlap "${thresholdBase}"
selectivesearch export-trec "${thresholdBase}"
trec_eval -q "${qrels}" "${thresholdBase}.trec" > "${thresholdBase}.trec.eval"

shift

done