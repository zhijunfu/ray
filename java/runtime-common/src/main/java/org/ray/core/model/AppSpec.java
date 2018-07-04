package org.ray.core.model;

import org.ray.util.config.AConfig;

/**
 * App specification.
 */
public class AppSpec {

  @AConfig(comment = "name for the package zip file for the app which is run as sriver")
  public String packageName;

  @AConfig(comment = "class name for the app")
  public String className;


  public AppSpec(String appPackageName, String appClassName) {
    packageName = appPackageName;
    className = appClassName;
  }
}
