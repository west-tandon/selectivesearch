#!/bin/bash

if [ -z "$1" ]; then
    while read line; do wc -w <<< "$line"; done
else
    cat "$1" | while read line; do wc -w <<< "$line"; done
fi;

