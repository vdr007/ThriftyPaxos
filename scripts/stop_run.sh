#!/bin/bash
mode=$1

if [[ $mode = "client" ]]; then
    pdsh -w node220-19 "sudo pkill java"
elif [[ $mode = "server" ]]; then
    pdsh -w node730-[1-3] "sudo pkill java"
else
    echo -e "please try cleaning client or server\n"
    exit 1
fi
