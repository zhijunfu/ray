#!/bin/bash

#############################
# build deploy file and deploy cluster
sh cleanup.sh
rm -rf local_deploy
./prepare.sh -t local_deploy
pushd local_deploy
local_ip=`ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
echo "use local_ip" $local_ip
# ray.java.start.node_ip_address=127.0.0.1
OVERWRITE="ray.java.start.redis_port=34222;ray.java.start.node_ip_address=$local_ip;ray.java.start.deploy=true;ray.java.start.run_mode=CLUSTER"
echo OVERWRITE is $OVERWRITE
./run.sh --head --overwrite=$OVERWRITE > cli.log 2>&1 &
popd
sleep 10

# auto-pack zip for app example
pushd example
if [ ! -d "app1/" ];then
    mkdir app1
fi
cp -rf target/ray-example-1.0.jar app1/
zip -r app1.zip app1
#zip target/ray-example-1.0.jar.zip target/ray-example-1.0.jar

# run with cluster mode
export RAY_CONFIG=../ray.config.ini
#echo "RUNNING echo VIA app proxy ...."
java -Djava.library.path=../../build/src/plasma:../../build/src/local_scheduler -cp .:target/ray-example-1.0.jar:lib/* org.ray.example.HelloWorld  --overwrite="ray.java.start.redis_address=$local_ip:34222;ray.java.start.run_mode=CLUSTER" --package=app1.zip --class=org.ray.example.HelloWorld
popd
