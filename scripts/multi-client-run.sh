#!/bin/bash
## run over 13-17 nodes
#pdsh -w node220-[13] "/users/shir/myproject/glassPaxos/wrapper-client.sh"
pdsh -w node220-[13-17] "/users/shir/myproject/glassPaxos/wrapper-client.sh"


#pdsh -w node220-[3-7] "/users/shir/myproject/glassPaxos/wrapper-client.sh"
#pdsh -w node220-[16-17] "/users/shir/myproject/glassPaxos/wrapper-client.sh"
