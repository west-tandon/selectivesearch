#!/bin/bash

#
# Arguments:
# 1) basename
#

basename=$1

if [ -z "${basename}" ]; then echo "You have to define basename (1)."; exit 1; fi;

find "${basename}*overlap" | while read overlapFile;
do
    len=`wc -l ${payoffFile} | cut -d" " -f1`
    costFile=`echo ${payoffFile} | sed "s/payoff$/cost/"`
    for i in `seq 1 ${len}`
    do
        echo $cost
    done > ${costFile}
done