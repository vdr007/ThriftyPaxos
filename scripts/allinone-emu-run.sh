#!/bin/bash
rundir=/users/shir/myproject/glassPaxos

rm -rf $rundir/logs;mkdir $rundir/logs
for i in 1 2 3; do
    $rundir/create_log_snapshot_dir.sh delete $i
    $rundir/create_log_snapshot_dir.sh create $i
    du -sh /mnt/zklogs/acceptorlog/
    $rundir/runTest.sh server $i all
done
