package io.warp10.spark;

import io.warp10.warp.sdk.WarpScriptExtension;

import java.util.HashMap;
import java.util.Map;

public class SparkWarpScriptExtension extends WarpScriptExtension {
  
  public static final String TOSPARK = "->SPARK";
  public static final String SPARKTO = "SPARK->";
  public static final String TOSPARKROW = "->SPARKROW";
  
  private static final Map<String,Object> functions;
  
  static {
    functions = new HashMap<String, Object>();
    
    functions.put(TOSPARK, new TOSPARK(TOSPARK));
    functions.put(SPARKTO, new SPARKTO(SPARKTO));
    functions.put(TOSPARKROW, new TOSPARKROW(TOSPARKROW));
    functions.put("PRINT", new PRINT("PRINT"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
