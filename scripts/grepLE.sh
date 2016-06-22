#!/bin/bash
## grep log related to leader election ##
#egrep -wi --color 'replace|suitable|=> backupSet|learner inActiveSet|sleep|receive sleep down|receive SleepDownMessage' logs/ -r

egrep -wi --color 'replace|suitable|=> backupSet|learner inActiveSet|sleep|receive sleep down|receive SleepDownMessage' *.log
