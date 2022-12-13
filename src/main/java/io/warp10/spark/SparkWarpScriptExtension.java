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

import io.warp10.script.functions.DUMP;
import io.warp10.warp.sdk.WarpScriptExtension;

import java.util.HashMap;
import java.util.Map;

public class SparkWarpScriptExtension extends WarpScriptExtension {

  public static final String TOSPARK = "->SPARK";
  public static final String SPARKTO = "SPARK->";
  public static final String TOSPARKROW = "->SPARKROW";
  public static final String TOSPARKTUPLE2 = "->SPARKTUPLE2";
  public static final String SPARKSCHEMA = "SPARKSCHEMA";

  private static final Map<String,Object> functions;

  static {
    functions = new HashMap<String, Object>();

    functions.put(TOSPARK, new TOSPARK(TOSPARK));
    functions.put(SPARKTO, new SPARKTO(SPARKTO));
    functions.put(TOSPARKROW, new TOSPARKROW(TOSPARKROW));
    functions.put(TOSPARKTUPLE2, new TOSPARKTUPLE2(TOSPARKTUPLE2));
    functions.put("ROWCOLS", new ROWCOLS("ROWCOLS"));
    functions.put("PRINT", new PRINT("PRINT"));
    functions.put(SPARKSCHEMA, new SPARKSCHEMA(SPARKSCHEMA));
    // Expose DUMP
    functions.put("DUMP", new DUMP("DUMP"));
  }

  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
