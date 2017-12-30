#!/bin/bash

input=$1
output=$2
shardCount=$3
column=$4

if [ -z "${input}" ]; then echo "You have to define input file (1)."; exit 1; fi;
if [ -z "${output}" ]; then echo "You have to define output file prefix (2)."; exit 1; fi;
if [ -z "${shardCount}" ]; then echo "You have to define shard count (3)."; exit 1; fi;
if [ -z "${column}" ]; then echo "You have to define column name (4)."; exit 1; fi;

for ((shard = 0; shard < ${shardCount}; shard++))
do
    pdsql \
        ${input} \
        -q "select query, shard, 0 as bucket, ${column} as impact from df0 where shard=${shard}" \
        -o "${output}#${shard}.impacts" \
        -d query=int32 shard=int32 bucket=int32
done
