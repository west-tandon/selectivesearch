#!/bin/bash

#
# Arguments:
# 1) basename
# 2) qrels
# *) budget(s)
#

basename=$1
qrels=$2
k=$3
shift 3

if [ -z "${basename}" ]; then echo "You have to define basename (1)."; exit 1; fi;
if [ -z "${qrels}" ]; then echo "You have to define qrels (2)."; exit 1; fi;
if [ -z "${k}" ]; then echo "You have to define k."; exit 1; fi;
if [ "$#" == "0" ]; then echo "You have to define budget(s) to test."; exit 1; fi

set -x
set -e

while (( "$#" )); do

budget=$1

selectivesearch select-opt ${basename} --budget ${budget} -k ${k}

budgetBase="${basename}\$[${budget}]"

selectivesearch selection2time "${budgetBase}"
selectivesearch overlap "${budgetBase}"
selectivesearch export-trec "${budgetBase}"
trec_eval -q "${qrels}" "${budgetBase}.trec" > "${budgetBase}.trec.eval"

shift

done