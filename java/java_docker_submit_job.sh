#!/bin/bash

#############################
# build deploy file and deploy cluster

# auto-pack zip for app example
pushd example
if [ ! -d "app1/" ];then
    mkdir app1
fi
cp -rf target/ray-example-1.0.jar app1/
zip -r app1.zip app1
popd

# run with cluster mode
pushd local_deploy
export RAY_CONFIG=ray/ray.config.ini
#ARGS=" --package ../example/app1.zip --class org.ray.example.HelloWorld --args=test1,test2  --redis-address=$local_ip:34222"
ARGS=" --package ../example/app1.zip --class org.ray.example.PlasmaQueuePerf  --redis-address=172.17.0.3:34222"
../local_deploy/run.sh submit $ARGS
popd
