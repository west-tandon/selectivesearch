#!/bin/bash

#
# Arguments:
# 1) basename
#

basename=$1

if [ -z "${basename}" ]; then echo "You have to define basename (1)."; exit 1; fi;

ls ${basename}*cost | while read costFile;
do
    len=`wc -l ${costFile} | cut -d" " -f1`
    impactFile=`echo ${costFile} | sed "s/cost$/payoff/"`
    shard=`expr ${costFile} : '.*#\(.*\)#'`
    for i in `seq 1 ${len}`
    do
        echo $((100 - $shard))
    done > ${impactFile}
done
