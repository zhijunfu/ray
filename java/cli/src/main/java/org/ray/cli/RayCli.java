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

  private static RunManager startProcess(CommandStart cmdStart, ConfigReader config) {
    PathConfig paths = new PathConfig(config);
    RayParameters params = new RayParameters(config);

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

        if (!manager.tryRecoverDeadProcess()) {
          RayLog.core.error("Restart all the processes in this node.");
          manager.cleanup(true);
          manager = startProcess(cmdStart, reader);
        }
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
