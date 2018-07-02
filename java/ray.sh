#!/bin/bash
realpath() {
    [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}
scripts_path=`realpath $0`
ray_dir=`dirname $scripts_path`
ray_dir=`dirname $ray_dir`

# echo "ray_dir = $ray_dir"

function usage() {
    echo "Options for sub-commands 'prepare|deploy|start|stop|clean|scds(stop-clean-deploy-start)'"
    echo " -s|--source-dir <dir> local source directory after 'ray.sh prepare dir', inside which a machines.txt"
    echo "                       exists, and its format is described below "
    echo " -t|--target-dir <dir> remote target directory for deployment, or local target directory for prepare a "
    echo "                       Ray cluster deployment package"
    echo " -f|--machine-file     a text file describing the cluster info and being copied as machines.txt"
    echo " -m|--mock             print trace log and exit without executing commands"
    echo "-------------------------------------------------------------------------------------------------------"
    echo "Each line of the machines.txt tells a machine and process arguments passed to the ray master tool which"
    echo "starts and stops the processes appropriately with the given arguments. Following is an example:"
    echo "   machine1: --route --redis-port 43222"
    echo "   machine2: --control --redis-address machine1:43222"
    echo "   machine3: --schedule --redis-address machine1:43222"
    echo "   machine4: --work --redis-address machine1:43222"
    echo "which invokes './start.sh --route --redis-port 43222' on 'machine1', and so on."
}

CMD=$1
shift

while [ $# -gt 0 ];do
#TODO: this may cause infinit loop when parameters are invalid
    key=$1
    case $key in
        -h|--help)
            usage
            exit 0
            ;;
        -s|--source-dir)
            s_dir=$2
            shift 2
            ;;
        -t|--target-dir)
            t_dir=$2
            shift 2
            ;;
        -f|--machine-file)
            m_file=$2
            shift 2
            ;;
        -m|--mock)
            mock="echo ----"
            shift
            ;;
        *)
            echo "ERROR: unknown option $key"
            echo
            usage
            exit -1
            ;;
    esac
done

#$1 machine $2 app $3 sdir $4 tdir
function deploy_files(){
    echo "deploy $2 from $3 to $4 at $1"
    $mock ssh -n $1 "mkdir -p $4"
    $mock rsync -a --exclude=machines.txt $3/* $1:$4
}

#$1 machine $2 app $3 sdir $4 tdir
function clean_files(){
    echo "cleaning $2 in $4 at $1"
    $mock ssh -n $1 "rm -fr $4"
}

#$1 machine $2 app $3 sdir $4 tdir $5 args
function start_server(){
    echo "starting $2 at $1"
    $mock ssh -n $1 "source ~/.bash_profile; cd $4; nohup sh -c \"(( ./start.sh $5 >foo.out 2>foo.err </dev/null)&)\""
    if [[ "$5" =~ "--head" || "$5" =~ "--route" ]]; then
        sleep 3
    fi
}

#$1 machine $2 app $3 sdir $4 tdir $5 args
function stop_server(){
    echo "stopping $2 at $1"
    $mock ssh -n $1 "source ~/.bash_profile; cd $4; nohup sh -c \"(( ./stop.sh >foo.out 2>foo.err </dev/null)&)\""
}

#$1 cmd $2 app $3 sdir $4 tdir
function run_one()
{
    if [ ! -e $3/machines.txt ];then
        echo "$3/machines.txt not ready"
        exit -1
    fi

    while read entry; do
        mac=`echo $entry | cut -d : -f 1`
        args=`echo $entry | cut -d : -f 2-`
        if [ -n "$mac" ]; then
            $1 $mac $2 $3 $4 "$args"
            echo ""
        fi
    done < $3/machines.txt
}

declare -a nativeBinaries=(
    "./src/common/thirdparty/redis/src/redis-server"
    "./src/plasma/plasma_store"
    #"./thirdparty/pkg/redis/src/redis-server"
    #"./thirdparty/pkg/arrow/cpp/build/cpp-install/bin/plasma_store"
    "./src/plasma/plasma_manager"
    "./src/local_scheduler/local_scheduler"
    "./src/global_scheduler/global_scheduler"
)

declare -a nativeLibraries=(
    "./src/common/redis_module/libray_redis_module.so"
    "./src/local_scheduler/liblocal_scheduler_library_java.dylib"
    "./src/plasma/libplasma_java.dylib"
)

declare -a javaBinaries=(
    "api"
    "common"
    "worker"
    "test"
)

function prepare_source()
{
    if [ -z $t_dir ];then
        echo "--target-dir not specified"
        usage
        exit -1
    fi

    # prepare native components under /ray/native/bin
    mkdir -p $t_dir"/ray/native/bin/"
    for i in "${!nativeBinaries[@]}"
    do
        cp $ray_dir/build/${nativeBinaries[$i]} $t_dir/ray/native/bin/
    done

    # prepare native libraries under /ray/native/lib
    mkdir -p $t_dir"/ray/native/lib/"
    for i in "${!nativeLibraries[@]}"
    do
        cp $ray_dir/build/${nativeLibraries[$i]} $t_dir/ray/native/lib/
        cp $ray_dir/thirdparty/pkg/arrow/lib/lib*.so.* $t_dir/ray/native/lib/
    done

    #cp $ray_dir/java/logmonitor.config.ini $t_dir/ray/native/bin/

    # prepare java components under /ray/java/lib
    mkdir -p $t_dir"/ray/java/lib/"
    #unzip -q master/target/ray-cli-ear.zip
    #cp ray-cli/lib/* $t_dir/ray/java/lib/
    #rm -rf ray-cli
    cp -rf $ray_dir/java/ray-cli/lib/* $t_dir/ray/java/lib/

    #mkdir -p $t_dir"/ray/java/pre_load_lib/"
    #if [ ! -f "$ray_dir/java/pre_load_lib.zip" ]; then
    #    osscmd get oss://raya/pre_load_lib.zip $ray_dir/java/pre_load_lib.zip
    #fi
    #unzip $ray_dir/java/pre_load_lib.zip -d $t_dir/ray/java/

    # prepare dsn config file
    cp -rf $ray_dir/java/ray.config.ini $t_dir/ray/

    # prepare java apps directory
    mkdir -p $t_dir"/ray/java/apps/"

    # prepare start.sh and stop.sh
    #cp $ray_dir/java/env.sh $t_dir/
    #cp $ray_dir/java/start.sh $t_dir/
    cp $ray_dir/java/run.sh $t_dir/
    #cp $ray_dir/java/stop.sh $t_dir/

    # copy python env
    #cp -r $ray_dir/java/python  $t_dir/ray/

    # prepare machines.txt
    #if [ $m_file ]; then
    #    cp $m_file $t_dir/machines.txt
    #fi
}

#$1 cmd
function run_()
{
    if [ -z $s_dir ] || [ -z $t_dir ]; then
        echo "--source-dir or --target-dir not specified"
        exit -1
    fi

    if [ ! -d $s_dir ];then
        echo "$s_dir no such directory"
        exit -1
    fi

    if [ -f ${s_dir}/apps.txt ]; then
        applist=$(cat ${s_dir}/apps.txt)
        for app in $applist;do
            run_one $1 $app ${s_dir}/$app ${t_dir}/$app
        done
    else
        run_one $1 $(basename "$s_dir") $s_dir $t_dir
    fi
}

case $CMD in
    prepare)
        prepare_source
        ;;
    start)
        run_ "start_server"
        ;;
    stop)
        run_ "stop_server"
        ;;
    deploy)
        run_ "deploy_files"
        ;;
    clean)
        run_ "stop_server"
        run_ "clean_files"
        ;;
    scds)
        run_ "stop_server"
        run_ "clean_files"
        run_ "deploy_files"
        run_ "start_server"
        ;;
    *)
        echo "Unkown command"
        usage
        echo
        ;;
esac
