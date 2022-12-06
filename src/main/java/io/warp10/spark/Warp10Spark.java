//
//   Copyright 2018-2022  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
package io.warp10.spark;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.LogManager;

import org.apache.spark.SparkFiles;
import org.apache.spark.sql.SparkSession;

import io.warp10.WarpConfig;
import io.warp10.script.WarpScriptLib;

public class Warp10Spark {

  private static final String DISABLE_LOGGING = "disable.logging";

  public static void init() {
    try {
      if (null != SparkSession.active().conf().get(WarpConfig.WARP10_CONFIG, null)) {
        // Force the Java property to be set to the same config file so other parts of Warp 10 behave correctly
        // even though the config was set via Spark
        System.setProperty(WarpConfig.WARP10_CONFIG, SparkSession.active().conf().get(WarpConfig.WARP10_CONFIG));
        InputStream in = Warp10Spark.class.getClassLoader().getResourceAsStream(SparkSession.active().conf().get(WarpConfig.WARP10_CONFIG));

        if (null == in) {
          try {
            in = new FileInputStream(SparkFiles.get(SparkSession.active().conf().get(WarpConfig.WARP10_CONFIG)));
          } catch (IOException ioe) {
          }
        }

        if (null == in) {
          in = new FileInputStream(SparkSession.active().conf().get(WarpConfig.WARP10_CONFIG));
        }

        WarpConfig.safeSetProperties(new InputStreamReader(in));
      } else if (null != System.getProperty(WarpConfig.WARP10_CONFIG)) {
        InputStream in = Warp10Spark.class.getClassLoader().getResourceAsStream(System.getProperty(WarpConfig.WARP10_CONFIG));

        if (null == in) {
          try {
            in = new FileInputStream(SparkFiles.get(System.getProperty(WarpConfig.WARP10_CONFIG)));
          } catch (IOException ioe) {
          }
        }

        if (null == in) {
          in = new FileInputStream(System.getProperty(WarpConfig.WARP10_CONFIG));
        }

        WarpConfig.safeSetProperties(new InputStreamReader(in));
      //} else if (System.getenv(WarpConfig.WARP10_CONFIG_ENV))
      } else if (null != System.getenv("WARP10_CONFIG")) {
        InputStream in = Warp10Spark.class.getClassLoader().getResourceAsStream(System.getenv("WARP10_CONFIG"));

        if (null == in) {
          try {
            in = new FileInputStream(SparkFiles.get(System.getenv("WARP10_CONFIG")));
          } catch (IOException ioe) {
          }
        }

        if (null == in) {
          in = new FileInputStream(System.getenv("WARP10_CONFIG"));
        }

        WarpConfig.safeSetProperties(new InputStreamReader(in));
      } else {
        WarpConfig.safeSetProperties((String) null);
      }

      //
      // Register extensions
      //

      WarpScriptLib.registerExtensions();

      //
      // Disable logging
      //
      if ("true".equals(System.getProperty(DISABLE_LOGGING))) {
        LogManager.getLogManager().reset();
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
