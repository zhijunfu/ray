#!/bin/bash

#############################
# run cluster
# collect dependencies into ray-cli dir
rm -rf ray-cli
unzip cli/target/ray-cli-ear.zip
cp test/lib/* ray-cli/lib/
cp test/target/ray-test-1.0.jar ray-cli/lib/

# build deploy file and deploy cluster
sh cleanup.sh
rm -rf local_deploy
./ray.sh prepare -t local_deploy
pushd local_deploy
local_ip=`ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
echo "use local_ip" $local_ip
# ray.java.start.node_ip_address=127.0.0.1
OVERWRITE="ray.java.start.redis_port=34222;ray.java.start.deploy=true;ray.java.start.run_mode=CLUSTER;ray.java.start.supremeFO=true;ray.java.start.disable_process_failover=true;ray.java.start.node_index=0;ray.java.start.start_log_server=false;ray.java.start.app_cluster_name=rayCluster1-$local_ip"
echo OVERWRITE is $OVERWRITE
./run.sh --head --overwrite=$OVERWRITE > adm.log 2>&1 &
popd
sleep 10

# auto-pack zip for app example
#pushd app-example
#if [ ! -d "app1/" ];then
#    mkdir app1
#fi
#cp -rf target/ray-app-example-1.0.jar app1/
#zip -r app1.zip app1

# run with cluster mode
#export RAY_CONFIG=$CUR_DIR/ray.cluster.config.ini
#echo "RUNNING echo VIA app proxy ...."
#java  -ea  -Djava.library.path=../../thirdparty/pkg/rdsn/lib:../../build/lib  -classpath ../ray-cli/lib/*:../app-example/target/classes org.ray.app.examples.integrated.client.EchoClientApp
#popd
