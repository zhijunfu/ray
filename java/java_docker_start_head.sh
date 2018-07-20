#!/bin/bash

#############################
# build deploy file and deploy cluster
sh cleanup.sh
rm -rf local_deploy
./prepare.sh -t local_deploy
pushd local_deploy
local_ip=`ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
#echo "use local_ip" $local_ip
OVERWRITE="ray.java.start.redis_port=34222;ray.java.start.node_ip_address=$local_ip;ray.java.start.deploy=true;ray.java.start.run_mode=CLUSTER"
echo OVERWRITE is $OVERWRITE
./run.sh start --head --overwrite=$OVERWRITE > cli.log 2>&1 &
popd
sleep 10
