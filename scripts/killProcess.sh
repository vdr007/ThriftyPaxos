#!/bin/bash
if [ $# -ne 2 ]; then
  echo -e "usage: killProcess.sh [hostname] [processname]"
  echo -e "hostname: e.g. node220-18"
  echo -e "processname: HashTableServer, Acceptor or all\n"
  exit 1
fi

learnerlogdir=/home/hadoop-shir/learnerlog
catchuplealogdir=/home/hadoop-shir/catchuplearnerlog
acceptorlogdir=/home/hadoop-shir/acceptorlog
fakesnapshot=/paxostest/fakeSnapshot.4G
hostname=$1
processname=$2
str=`ssh $hostname "jps | grep $processname"`
x=$(awk '{print $1}' <<< ${str})
#echo -e "$processname on $hostname id: $x\n"
if [ "$processname" == "all" ]; then
    ssh $hostname "sudo pkill java"
else
    ssh $hostname "sudo kill $x"
fi
#ssh $hostname "sudo kill `jps | grep Acceptor | awk '{print $1}'`"

# cleanup learner log
if [ "$processname" == "HashTableServer" ] || [ "$processname" == "all" ]; then
    ssh $hostname "sudo rm -rf $learnerlogdir $catchuplealogdir $fakesnapshot; mkdir -p $learnerlogdir $catchuplealogdir"
    ssh $hostname "du -sh $learnerlogdir $catchuplealogdir" 
fi

# if [ "$processname" == "Acceptor" ] || [ "$processname" == "all" ]; then
#     ssh $hostname "sudo rm -rf $acceptorlogdir; mkdir -p $acceptorlogdir"
# fi
