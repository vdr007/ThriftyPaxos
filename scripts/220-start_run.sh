#!/bin/bash
mode=$1
rank=$2

baseacc=/home/hadoop-shir
baselea=/paxostest
rundir=/users/shir/myproject/glassPaxos
host=`hostname`

if [[ $mode = "client" ]]; then
    pdsh -w $host "$rundir/runTest.sh $mode $rank"
elif [[ $mode = "server" ]]; then
    rm -rf $rundir/logs;mkdir $rundir/logs
    # pdsh -w node220-[6-8] "$rundir/create_log_snapshot_dir.sh delete"
    # pdsh -w node220-[6-8] "$rundir/create_log_snapshot_dir.sh create"
    # pdsh -w node220-[6-8] "du -sh $baseacc/* $baselea/snapshots"
    pdsh -w node220-[18-20] "$rundir/create_log_snapshot_dir.sh delete"
    pdsh -w node220-[18-20] "$rundir/create_log_snapshot_dir.sh create"
    pdsh -w node220-[18-20] "du -sh $baseacc/* $baselea/snapshots"
#pdsh -w node730-[1-3] "$rundir/runTest.sh"
else
    echo -e "please try start client or server\n"
    exit 1
fi
