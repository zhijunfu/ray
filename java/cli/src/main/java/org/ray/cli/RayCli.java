package org.ray.cli;

import com.beust.jcommander.JCommander;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import org.ray.cli.CommandStart;
import org.ray.cli.CommandStop;
import org.ray.core.model.RayParameters;
import org.ray.runner.RunInfo;
import org.ray.runner.RunManager;
import org.ray.spi.PathConfig;
import org.ray.util.NetworkUtil;
import org.ray.util.config.ConfigReader;
import org.ray.util.logger.RayLog;

/**
 * Ray command line interface
 */
public class RayCli {

  private static RayCliArgs rayArgs = new RayCliArgs();

  private static RunManager startRayHead(RayParameters params, PathConfig paths,
      ConfigReader configReader) {
    RunManager manager = new RunManager(params, paths, configReader);

    try {
      manager.startRayHead();
    } catch (Exception e) {
      e.printStackTrace();
      RayLog.core.error("error at RayCli startRayHead", e);
      throw new RuntimeException("Ray head node start failed, err = " + e.getMessage());
    }

    RayLog.core.info("Started Ray head node. Redis address: " + manager.info().redisAddress);
    return manager;
  }

  private static RunManager startRayNode(RayParameters params, PathConfig paths,
      ConfigReader configReader) {
    RunManager manager = new RunManager(params, paths, configReader);

    try {
      manager.startRayNode();
    } catch (Exception e) {
      e.printStackTrace();
      RayLog.core.error("error at RayCli startRayNode", e);
      throw new RuntimeException("Ray work node start failed, err = " + e.getMessage());
    }

    RayLog.core.info("Started Ray work node.");
    return manager;
  }

  private static RunManager startUnitNode(
      RayParameters params,
      PathConfig paths,
      ConfigReader configReader,
      boolean isRoute, boolean isControl, boolean isSchedule) {
    RunManager manager = new RunManager(params, paths, configReader);

    params.num_local_schedulers = 0;

    if (params.node_ip_address.length() == 0) {
      params.node_ip_address = NetworkUtil.getIpAddress(null);
    }

    if (isRoute && params.redis_address.length() != 0) {
      throw new RuntimeException("Redis address must be empty in route node.");
    }
    if (!isRoute && params.redis_address.length() == 0) {
      throw new RuntimeException("Redis address cannot be empty in non-route node.");
    }

    if (isControl) {
      if (params.num_redis_shards <= 0) {
        params.num_redis_shards = 1;
      }
      params.start_redis_shards = true;
    } else {
      if (params.num_redis_shards != 0) {
        throw new RuntimeException("Number of redis shards should be zero in non-control node.");
      }
      params.start_redis_shards = false;
    }

    if (isSchedule) {
      params.include_global_scheduler = true;
    } else {
      params.include_global_scheduler = false;
    }

    try {
      manager.startRayProcesses();
    } catch (Exception e) {
      e.printStackTrace();
      RayLog.core.error("error at RayCli startUnitNode", e);
      throw new RuntimeException("Ray unit node start failed, err = " + e.getMessage());
    }

    RayLog.core.info("Started Ray unit node. Route node is " + isRoute +
        ", control node is " + isControl + ", schedule node is " + isSchedule + ".");
    return manager;
  }

  private static RunManager startProcess(CommandStart cmdStart, ConfigReader config) {
    PathConfig paths = new PathConfig(config);
    RayParameters params = new RayParameters(config);
    //params.run_mode = RunMode.CLUSTER;
    //params.node_ip_address = cmdStart.node_ip_address;
    //params.redis_address = cmdStart.redis_address;
    //params.redis_port = cmdStart.redis_port;
    //params.num_redis_shards = cmdStart.num_redis_shards;
    //params.num_workers = cmdStart.num_workers;
    //params.num_local_schedulers = 1;
    //params.num_cpus = new int[]{cmdStart.num_cpus};
    //params.num_gpus = new int[]{cmdStart.num_gpus};
    //params.deploy = cmdStart.deploy;
    //params.yarn = cmdStart.yarn;
    //params.working_directory = cmdStart.working_dir;
    //params.logging_directory = cmdStart.logging_dir;

    // Get the node IP address if one is not provided.
    //if (params.node_ip_address.length() == 0) {
    //    params.node_ip_address = NetworkUtil.getIpAddress(null);
    //}
    RayLog.core.info("Using IP address " + params.node_ip_address + " for this node.");
    RunManager manager;
    if (cmdStart.head) {
      manager = startRayHead(params, paths, config);
    } else if (cmdStart.work) {
      manager = startRayNode(params, paths, config);
    } else {
      throw new RuntimeException(
          "Ray must be started with at least a role. Seeing --help for more details.");
    }
    return manager;
  }

  private static void start(CommandStart cmdStart, ConfigReader reader) {
    RayParameters params = new RayParameters(reader);

    RunManager manager = null;

    manager = startProcess(cmdStart, reader);

    // monitoring all processes, throwing an exception when any process fails
    while (true) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
      }

      HashSet<RunInfo.ProcessType> excludeTypes = new HashSet<>();
      if (!manager.checkAlive(excludeTypes)) {

        //ray components fail-over
        RayLog.core.error("Something error in Ray processes.");
        if (params.disable_process_failover) {
          RayLog.core.error("process failover is disable, ignore the fail process");
        } else {
          if (params.supremeFO) {
            RayLog.core.error("Start supreme failover, kill myself now");
            System.exit(-1);
          }
          if (!manager.tryRecoverDeadProcess()) {
            RayLog.core.error("Restart all the processes in this node.");
            manager.cleanup(true);
            manager = startProcess(cmdStart, reader);
          }
        }

        //throw new RuntimeException("Something error in Ray processes.");
      }
    }
  }

  private static void stop(CommandStop cmdStop) {
    String[] cmd = {"/bin/sh", "-c", ""};

    cmd[2] = "killall global_scheduler local_scheduler plasma_store plasma_manager";
    try {
      Runtime.getRuntime().exec(cmd);
    } catch (IOException e) {
    }

    cmd[2] = "kill $(ps aux | grep monitor.py | grep -v grep | " +
        "awk \'{ print $2 }\') 2> /dev/null";
    try {
      Runtime.getRuntime().exec(cmd);
    } catch (IOException e) {
    }

    cmd[2] = "kill $(ps aux | grep redis-server | grep -v grep | " +
        "awk \'{ print $2 }\') 2> /dev/null";
    try {
      Runtime.getRuntime().exec(cmd);
    } catch (IOException e) {
    }

    cmd[2] = "kill -9 $(ps aux | grep DefaultWorker | grep -v grep | " +
        "awk \'{ print $2 }\') 2> /dev/null";
    try {
      Runtime.getRuntime().exec(cmd);
    } catch (IOException e) {
    }

    cmd[2] = "kill $(ps aux | grep log_monitor.py | grep -v grep | " +
        "awk \'{ print $2 }\') 2> /dev/null";
    try {
      Runtime.getRuntime().exec(cmd);
    } catch (IOException e) {
    }

    cmd[2] = "kill $(ps aux | grep AppProxy | grep -v grep | " +
        "awk \'{ print $2 }\') 2> /dev/null";
    try {
      Runtime.getRuntime().exec(cmd);
    } catch (IOException e) {
    }
  }

  public static void main(String[] args) throws Exception {

    CommandStart cmdStart = new CommandStart();
    CommandStop cmdStop = new CommandStop();
    JCommander rayCommander = JCommander.newBuilder().addObject(rayArgs)
        .addCommand("start", cmdStart)
        .addCommand("stop", cmdStop)
        .build();
    rayCommander.parse(args);

    if (rayArgs.help) {
      rayCommander.usage();
      System.exit(0);
    }

    String cmd = rayCommander.getParsedCommand();
    if (cmd == null) {
      rayCommander.usage();
      System.exit(0);
    }

    String configPath;
    if (cmdStart.config != null && !cmdStart.config.equals("")) {
      configPath = cmdStart.config;
    } else {
      configPath = System.getenv("RAY_CONFIG");
      if (configPath == null) {
        configPath = System.getProperty("ray.config");
      }
      if (configPath == null) {
        throw new Exception("Please set config file path in env RAY_CONFIG or property ray.config");
      }
    }

    switch (cmd) {
      case "start": {
        // TODO: fix me
        ConfigReader config = new ConfigReader(configPath, cmdStart.overwrite);
        start(cmdStart, config);
      }
      break;
      case "stop":
        stop(cmdStop);
        break;
      default:
        rayCommander.usage();
    }
  }

}
