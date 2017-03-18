package io.warp10.spark;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.spark.SparkFiles;

import io.warp10.WarpConfig;
import io.warp10.script.WarpScriptLib;

public class Warp10Spark {
  public static void init() {
    try {
      if (null != System.getProperty(WarpConfig.WARP10_CONFIG)) {
        InputStream in = Warp10Spark.class.getClassLoader().getResourceAsStream(System.getProperty(WarpConfig.WARP10_CONFIG));
        
        if (null == in) {
          in = new FileInputStream(SparkFiles.get(System.getProperty(WarpConfig.WARP10_CONFIG)));
        }
        WarpConfig.safeSetProperties(new InputStreamReader(in));
      } else {
        WarpConfig.safeSetProperties((String) null);
      }
      
      //
      // Register extensions
      //
      
      WarpScriptLib.registerExtensions();      
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
