#!/bin/bash
mode=$1
rank=$2
#basedir=/mnt/zklogs
basedir=/paxostest
rundir=/users/shir/myproject/glassPaxos

if [[ $mode = "client" ]]; then
    pdsh -w node220-19 "$rundir/runTest.sh $mode $rank"
elif [[ $mode = "server" ]]; then
    rm -rf $rundir/logs;mkdir $rundir/logs
    pdsh -w node730-[1-3] "$rundir/create_log_snapshot_dir.sh delete"
    pdsh -w node730-[1-3] "$rundir/create_log_snapshot_dir.sh create"
    pdsh -w node730-[1-3] "du -sh $basedir/"
#pdsh -w node730-[1-3] "$rundir/runTest.sh"
else
    echo -e "please try start client or server\n"
    exit 1
fi
