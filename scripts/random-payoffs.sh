#!/bin/bash

#
# Arguments:
# 1) basename
# 2) shard count
# 3) bucket count
# 4) query count
#

basename=$1
shardCount=$2
bucketCount=$3
queryCount=$4

if [ -z "${basename}" ]; then echo "You have to define basename (1)."; exit 1; fi;
if [ -z "${shardCount}" ]; then echo "You have to define shard count (2)."; exit 1; fi;
if [ -z "${bucketCount}" ]; then echo "You have to define bucket count (3)."; exit 1; fi;
if [ -z "${queryCount}" ]; then echo "You have to define query count (4)."; exit 1; fi;

rm -f *payoff

for ((shard = 0; shard < ${shardCount}; shard++))
do
    for ((bucket = 0; bucket < ${bucketCount}; bucket++))
    do
        for ((query = 0; query < ${queryCount}; query++))
        do
            echo ${RANDOM} >> "${basename}#${shard}#${bucket}.payoff"
        done
    done
done