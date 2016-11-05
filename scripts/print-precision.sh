#!/bin/bash

#
# Arguments:
# 1) basename
#

basename=$1

if [ -z "${basename}" ]; then echo "You have to define basename (1)."; exit 1; fi;

find "${basename}" -name "*.trec.eval" | while read trecEvalFile;
do
    regex="\[(.*)\]"
    if [[ "${trecEvalFile}" =~ ${regex} ]]; then
        budget="${BASH_REMATCH[1]}"
        echo "Precision with budget ${budget}"
        tail -9 ${trecEvalFile} | head -6
    fi
done