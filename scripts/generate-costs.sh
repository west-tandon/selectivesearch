#!/bin/bash

#
# Arguments:
# 1) basename
#

basename=$1
cost=$2

if [ -z "${basename}" ]; then echo "You have to define basename (1)."; exit 1; fi;
if [ -z "${cost}" ]; then echo "You have to define cost (2)."; exit 1; fi;

ls ${basename}*payoff | while read payoffFile;
do
    len=`wc -l ${payoffFile} | cut -d" " -f1`
    costFile=`echo ${payoffFile} | sed "s/payoff$/cost/"`
    for i in `seq 1 ${len}`
    do
        echo $cost
    done > ${costFile}
done
