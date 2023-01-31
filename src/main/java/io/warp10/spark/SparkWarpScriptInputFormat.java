//
//   Copyright 2018-2023  SenX S.A.S.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import org.apache.spark.SparkFiles;

import io.warp10.hadoop.WarpScriptInputFormat;
import io.warp10.spark.common.SparkUtils;

public class SparkWarpScriptInputFormat extends WarpScriptInputFormat {

  static {
    Warp10Spark.init();
  }

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
