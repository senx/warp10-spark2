package io.warp10.spark;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import org.apache.spark.SparkFiles;

import io.warp10.hadoop.WarpScriptInputFormat;
import io.warp10.spark.common.SparkUtils;

public class SparkWarpScriptInputFormat extends WarpScriptInputFormat {
  @Override
  public InputStream getWarpScriptInputStream(String originalFilePath) throws IOException {
    String filename = Paths.get(originalFilePath).getFileName().toString();

    File f = new File(SparkFiles.get(filename));
    
    InputStream fis = null;
    
    if (!f.exists()) {
      fis = SparkUtils.class.getClassLoader().getResourceAsStream(filename);
    }
    
    if (null == fis) {
      fis = new FileInputStream(f);
    }
    
    if (null == fis) {
      throw new IOException("WarpScript file '" + originalFilePath + "' could not be found.");
    }

    return fis;
  }
}
