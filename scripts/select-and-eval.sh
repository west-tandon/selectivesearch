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
if [ "$#" == "0" ]; then echo "You have to define budget(s) to test."; exit 1; fi

set -x
set -e

while (( "$#" )); do

budget=$1

selectivesearch select-shards ${basename} --budget ${budget}

budgetBase="${basename}\$[${budget}]"

selectivesearch overlap "${budgetBase}"
selectivesearch export-trec "${budgetBase}"
trec_eval -q "${qrels}" "${budgetBase}.trec" > "${budgetBase}.trec.eval"

shift

done