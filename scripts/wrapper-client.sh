#!/bin/bash

rundir=/users/shir/myproject/glassPaxos
#ssh node220-13 "$rundir/runTest.sh client 2"

hname=`hostname`
##host1=node220-16
##host2=node220-17
host1=node220-13
host2=node220-14
host3=node220-15
host4=node220-16
host5=node220-17

numhosts=5
rankstart=1
rankend=10


gap=$(($rankend/$numhosts))

myrank=
begin=
end=

## ranklist=($(seq $begin $end))
## for rk in "${ranklist[@]}"; do
## done

#if [ $hname = $host1 ]; then
#fi

case $hname in
$host1)
    myrank=1
    ;;
$host2)
    myrank=2
    ;;
$host3)
    myrank=3
    ;;
$host4)
    myrank=4
    ;;
$host5)
    myrank=5
    ;;
*)
    echo -e "please run on correct node\n"

esac

begin=$((1+$(($gap*$myrank-$gap))))
end=$(($gap*$myrank))
echo "gap=$gap, begin=$begin, end=$end"

for (( i=begin; i<=end; i++ )); do
    $rundir/runTest.sh client $i dummy
    echo " $rundir/runTest.sh client $i dummy"
done
