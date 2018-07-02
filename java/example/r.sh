#!/usr/bin/env bash
#first you should run the ../test.sh to build
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
export RAY_CONFIG=ray.config.ini
java -Djava.library.path=../../build/src/plasma:../../build/src/local_scheduler -cp .:target/ray-example-1.0.jar:lib/* org.ray.example.HelloWorld  --config=ray.config.ini  --overwrite="ray.java.start.redis-address=30.30.182.8:34222" --package=target/ray-example-1.0.jar.zip --class=org.ray.example.HelloWorld
