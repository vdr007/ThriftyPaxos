#!/bin/bash
mode=$1
#host=`hostname`
client=$2
server=$2
clientRank=1
serverRank=$3
rundir=/users/shir/myproject/glassPaxos

if [[ $mode = "client" ]]; then
    pdsh -w $client $rundir/runTest.sh $mode $clientRank
elif [[ $mode = "server" ]]; then
    pdsh -w $server rm -rf $rundir/logs;mkdir $rundir/logs
    pdsh -w $server $rundir/create_log_snapshot_dir.sh delete
    pdsh -w $server $rundir/create_log_snapshot_dir.sh create
    #pdsh -w $server du -sh /mnt/zklogs/acceptorlog/
    pdsh -w $server $rundir/runTest.sh $mode $serverRank acceptor
else
    echo -e "please try start client or server\n"
    exit 1
fi
