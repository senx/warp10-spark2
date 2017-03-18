package io.warp10.spark;

import java.io.IOException;
import java.io.InputStreamReader;

import io.warp10.WarpConfig;
import io.warp10.script.WarpScriptLib;

public class Warp10Spark {
  public static void init() throws IOException {
    if (null != System.getProperty(WarpConfig.WARP10_CONFIG)) {
      WarpConfig.setProperties(new InputStreamReader(Warp10Spark.class.getClassLoader().getResourceAsStream(System.getProperty(WarpConfig.WARP10_CONFIG))));
    } else {
      WarpConfig.setProperties((String) null);
    }
    
    //
    // Register extensions
    //
    
    WarpScriptLib.registerExtensions();
  }
}
