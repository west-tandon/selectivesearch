#!/usr/bin/env bash

# Arguments:
# 1) basename of the cluster

basename=$1
if [ -z "${basename}" ]; then echo "You have to define cluster basename (1)."; exit 1; fi;

ls ${basename}*-*titles | while read file;
do
        number=`basename ${file} | sed "s/.*-//" | sed "s/\..*//"`
        wc=`wc -l ${file} | cut -d" " -f1`
        echo "${number} ${wc}"
done | sort -n | cut -d" " -f2
