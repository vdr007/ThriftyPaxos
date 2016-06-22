#!/bin/bash
topdir=/users/shir/myproject/glassPaxos
rm -rf $topdir/clientRes/test1/*
cp -r $topdir/logs/client_log-* $topdir/clientRes/test1/
ls $topdir/clientRes/test1/ | wc -l
$topdir/calc-client.sh .

rpstmpdata=$topdir/clientRes/test1/rps-sumc.tmp
lattmpdata=$topdir/clientRes/test1/lat-sumc.tmp
for tmpdata in $rpstmpdata $lattmpdata; do
    cat $tmpdata | sed '1,20 d' | cat > tmpa
    cat tmpa | sed '120,$ d' | cat > tmpb
    wc -l $tmpdata
    wc -l tmpa
    wc -l tmpb
awk '{ sum+=$1} END {print sum/NR}' tmpb
done

rm $rpstmpdata $lattmpdata tmpa tmpb
#$topdir/clientRes/test1/*-tmp $topdir/clientRes/test1/sum.tmp

# manual check
#awk '{ sum+=$1} END {print sum/NR}' clientRes/test1/sumc.tmp
