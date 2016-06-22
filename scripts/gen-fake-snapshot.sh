#!/bin/bash

## generate fake k-v snapshot with fixed size (GB)

if [ $# -ne 1 ]; then
    echo -e "gen-fake-snapshot.sh [snapshot_size]\n"
    echo -e "snapshot_size: x GB\n"
    exit 1
fi
sssize=$1
java -cp lib/*:dist/lib/* -Xmx8192m -Xms8192m glassPaxos.apps.hashtable.CreateFakeSnapshot /paxostest/snapshots $sssize
