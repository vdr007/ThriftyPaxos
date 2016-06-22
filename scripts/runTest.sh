## build
# ant

## run
if [ $# -ne 3 ]; then
  echo -e "usage: runTest.sh [mode] [rank] [runmode]"
  echo -e "[mode]: start client or server"
  echo -e "[rank]: client or server rank"
  echo -e "[runmode]: server only, run acceptor, learner or all\n"
fi

mode=$1
rank=$2
runmode=$3

# client only, 512, 4096, 16384
valueLen=512

JAVARUN="java -cp lib/*:dist/lib/*"
CONF_FILE=/users/shir/myproject/glassPaxos/glasspaxos.conf
BASE_DIR=/users/shir/myproject/glassPaxos
LOG_DIR=${BASE_DIR}/logs
#host=`hostname`
host=$rank
#cli_log=${LOG_DIR}/client_log-$host
lea_log=${LOG_DIR}/learner_log-$host
acc_log=${LOG_DIR}/acceptor_log-$host
# rm -rf ${cli_log} ${lea_log} ${acc_log}
# mkdir -p ${cli_log} ${lea_log} ${acc_log}
cd $BASE_DIR

# clear previous threads
##sudo pkill java

#if [[ `hostname` = node220* ]]; then
#elif [[ `hostname` = node730* ]]; then

if [ "$mode" = "client" ]; then
    hname=`hostname`
    #rk=${hname:${#hname} - 2}
    #myrk=$((rk-12))
    myrk=$rank
    cli_log=${LOG_DIR}/client_log-$myrk
    echo -e "$hname rk=$rk myrk=$myrk, cliLog=${cli_log}"
    rm -rf ${cli_log}
    touch ${cli_log}
    #$JAVARUN -d64 -Xmx8192m -Xms8192m glassPaxos.client.Client $1 "$CONF_FILE" | tee -a ${cli_log} &
    $JAVARUN -Xmx256m -Xms256m glassPaxos.apps.hashtable.AppClient $myrk "$CONF_FILE" $valueLen 2>&1 | tee -a ${cli_log} &
elif [ "$mode" = "server" ]; then
    if [ "$runmode" == "acceptor" ]; then
        rm -rf ${acc_log}; touch ${acc_log}
        $JAVARUN -d64 -Xmx2048m -Xms2048m glassPaxos.acceptor.Acceptor $rank "$CONF_FILE" 2>&1 | tee -a ${acc_log} &
    elif [ "$runmode" == "learner" ]; then
        rm -rf ${lea_log}; touch ${lea_log}
        $JAVARUN -d64 -Xmx8192m -Xms8192m glassPaxos.apps.hashtable.HashTableServer $rank "$CONF_FILE" 2>&1 | tee -a ${lea_log} &
#$JAVARUN -d64 -Xmx8192m -Xms8192m glassPaxos.apps.hashtable.SimpleEchoServer $rank "$CONF_FILE" 2>&1 | tee -a ${lea_log} &
#$JAVARUN -d64 -Xmx8192m -Xms8192m glassPaxos.learner.Learner $rank "$CONF_FILE" 2>&1 | tee -a ${lea_log} &
    elif [ "$runmode" == "all" ]; then
        rm -rf ${lea_log} ${acc_log}; touch ${lea_log} ${acc_log}
        #"-agentlib:hprof=heap=sites,cpu=samples,monitor=y,depth=15,thread=y,file=logs/hprof-acc-$host.txt" \
        #"-agentlib:hprof=cpu=samples,monitor=y,depth=15,thread=y,interval=1,file=logs/hprof-acc-$host.txt" \
        $JAVARUN \
        -d64 -Xmx2048m -Xms2048m glassPaxos.acceptor.Acceptor $rank "$CONF_FILE" 2>&1 | tee -a ${acc_log} &
#if [[ `hostname` != "node730-3" ]]; then
        if [[ "$rank" != "7" ]]; then
            #"-agentlib:hprof=heap=sites,cpu=samples,monitor=y,depth=15,thread=y,file=logs/hprof-lea-$host.txt" \
            $JAVARUN \
            -d64 -Xmx8192m -Xms8192m glassPaxos.apps.hashtable.HashTableServer $rank "$CONF_FILE" 2>&1 | tee -a ${lea_log} &
        fi
        #$JAVARUN glassPaxos.rstest.PerformanceLearner $host "$CONF_FILE" &
        #$JAVARUN glassPaxos.rstest.PerformanceAcceptor $host "$CONF_FILE" &
    else
        echo -e "please select 'acceptor', 'learner' or 'all' runmode \n"
        exit 1
    fi
else
    echo -e "please launch on 220-x or 730-x nodes \n"
    exit 1
fi
