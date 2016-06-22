topdir=/users/shir/myproject/glassPaxos
rm *.log
pdsh -w node730-[1-3] "killall java"
ssh node730-1 "rm /paxostest/acceptorlog/* -rf; cd $topdir/src/applications/h2/; ./start.sh $1" &>learner1.log &
ssh node730-2 "rm /paxostest/acceptorlog/* -rf; cd $topdir/src/applications/h2/; ./start.sh $1" &>learner2.log &
ssh node730-3 "rm /paxostest/acceptorlog/* -rf; cd $topdir/src/applications/h2/; ./start.sh $1" &>learner3.log &

#sleep 30
ssh node730-1 "cd $topdir; ./start_run_single.sh server node730-1 1" &>acceptor1.log &
#sleep 5
ssh node730-2 "cd $topdir; ./start_run_single.sh server node730-2 2" &>acceptor2.log &
#sleep 5
ssh node730-3 "cd $topdir; ./start_run_single.sh server node730-3 3" &>acceptor3.log &
