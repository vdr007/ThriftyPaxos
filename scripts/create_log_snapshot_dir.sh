#!/bin/bash
mode=$1

#logdir=/users/shir/myproject/glassPaxos/acceptorlog
#basedir=/mnt/zklogs

## baseacc: /dev/sda, baselea: /dev/sdb
baseacc=/home/hadoop-shir
baselea=/paxostest
acceptorgcstatfile=$baseacc/acceptorGCStat

acceptorlogdir=$baseacc/acceptorlog
catchupacclogdir=$baseacc/catchupacceptorlog
learnerlogdir=$baseacc/learnerlog
catchuplealogdir=$baseacc/catchuplearnerlog
learnersnapshotdir=$baselea/snapshots

# for allinone
# rank=$2
# acceptorlog=/mnt/zklogs/acceptorlog/acceptor
# learnersnapshot=/mnt/zklogs/snapshots/learner
# acceptorlogdir=$acceptorlog$rank
# learnersnapshotdir=$learnersnapshot$rank

operation=
if [[ ${mode} = "create" ]]; then
    operation="mkdir -p $acceptorlogdir $catchupacclogdir $learnerlogdir $catchuplealogdir $learnersnapshotdir"
elif [[ ${mode} = "delete" ]]; then
#operation="sudo rm -rf $acceptorlogdir $learnerlogdir $learnersnapshotdir $acceptorgcstatfile"
    operation="sudo rm -rf $baseacc/* $learnersnapshotdir $acceptorgcstatfile"
else
    echo -e "please try create or delete\n"
    exit 1
fi

$operation
#pdsh -w node730-[1-3] $operation
# du -sh $logdir/*

# correct usage
# pdsh -w node730-[1-3] "/users/shir/myproject/glassPaxos/create_log_snapshot_dir.sh delete"
# pdsh -w node730-[1-3] "/users/shir/myproject/glassPaxos/create_log_snapshot_dir.sh create"
# pdsh -w node730-[1-3] "ls -l /mnt/zklogs/acceptorlog/"
