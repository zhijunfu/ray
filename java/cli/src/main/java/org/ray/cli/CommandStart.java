package org.ray.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Arguments for command start
 */
@Parameters(separators = "= ", commandDescription = "start ray daemons")
public class CommandStart {

  @Parameter(names = "--head", description = "start the head node")
  public boolean head;
/*
  @Parameter(names = "--route", description = "start the route node storing cluster info")
  public boolean route;

  @Parameter(names = "--control", description = "start the control node storing global control states")
  public boolean control;

  @Parameter(names = "--schedule", description = "start the global scheduler node")
  public boolean schedule;
*/
  @Parameter(names = "--work", description = "start the work node including local scheduler, plasma and worker")
  public boolean work;

  //@Parameter(names = "--node-ip-address", description = "the IP address of this node")
  //public String node_ip_address = "";

  //@Parameter(names = "--redis-address", description =
  //        "the address to use for connecting to Redis")
  //public String redis_address = "";

  //@Parameter(names = "--redis-port", description = "the port to use for starting Redis")
  //public int redis_port = 34222;

  //@Parameter(names = "--num-redis-shards", description =
  //        "the number of additional Redis shards to use in addition to the primary Redis shard")
  //public int num_redis_shards = 0;

  //@Parameter(names = "--object-manager-port", description =
  //        "the port to use for starting the object manager")
  //public int object_manager_port = 0;

  //@Parameter(names = "--num-workers", description =
  //        "the initial number of workers to start on this node")
  //public int num_workers = 0;

  //@Parameter(names = "--num-cpus", description = "the number of CPUs on this node")
  //public int num_cpus = 1;

  //@Parameter(names = "--num-gpus", description = "the number of GPUs on this node")
  //public int num_gpus = 1;
/*
  @Parameter(names = "--block",
      description = "provide this argument to block forever in this command")
  public boolean block;
*/
  //@Parameter(names = "--deploy", description = "using deploy paths, otherwise using project paths")
  //public boolean deploy;

  //@Parameter(names = "--yarn", description = "indicate yarn deployment under which all logs are output to working dir")
  //public boolean yarn;

  //@Parameter(names = "--working-dir", description = "root working directory of Ray processes")
  //public String working_dir = "";

  //@Parameter(names = "--logging-dir", description = "root logging directory of Ray processes")
  //public String logging_dir = "";

  //@Parameter(names = "--py", description = "Switch to ray python")
  //public boolean py;
/*
  @Parameter(names = "--app-proxy", description = "Start app proxy")
  public boolean startAppProxy;
*/
  @Parameter(names = "--config", description = "the config file of raya")
  public String config = "";

  @Parameter(names = "--overwrite", description = "the overwrite items of config")
  public String overwrite = "";

}
