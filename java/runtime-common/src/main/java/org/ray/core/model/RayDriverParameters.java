package org.ray.core.model;

import org.ray.util.config.AConfig;

/**
 * Runtime parameters of Ray process.
 */
public class RayDriverParameters {

  @AConfig(comment = "name for the package zip file for the driver")
  public String packageName;

  @AConfig(comment = "class name for the driver")
  public String className;


  public RayDriverParameters(String appPackageName, String appClassName) {
    packageName = appPackageName;
    className = appClassName;
  }
}
