package io.warp10.spark;

import org.apache.spark.SparkConf;

import java.util.Map;

public class PySparkLauncher {

  public void testPySparkLauncher(Map<String,String> kv) {
    try {
      System.out.println("testPySparkLauncher");
      SparkTest sparkTest = new SparkTest();
      SparkConf conf = new SparkConf().setAppName("testPySparkLauncher");

      kv.forEach((k, v) -> {
        System.out.println(k + " -> " + v);
        conf.set(k,v);
      });

      sparkTest.testParquet(conf);
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }
}
