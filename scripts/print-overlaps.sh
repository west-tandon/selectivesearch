#!/bin/bash

#
# Arguments:
# 1) basename
#

basename=$1

if [ -z "${basename}" ]; then echo "You have to define basename (1)."; exit 1; fi;

find "${basename}" -name "*.overlap" | while read overlapFile;
do
    regex="\[(.*)\]@([0-9]+)"
    if [[ "${overlapFile}" =~ ${regex} ]]; then
        budget="${BASH_REMATCH[1]}"
        overlap="${BASH_REMATCH[2]}"
        printf "Overlap@${overlap} with budget ${budget}:\t"
        cat ${overlapFile}
    fi
done