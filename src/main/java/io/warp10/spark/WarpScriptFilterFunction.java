package io.warp10.spark;

import io.warp10.script.WarpScriptException;
import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;

import org.apache.spark.api.java.function.FilterFunction;

import java.util.ArrayList;
import java.util.List;

public class WarpScriptFilterFunction<T> extends WarpScriptAbstractFunction implements FilterFunction<T> {

  public WarpScriptFilterFunction(String code) throws WarpScriptException {
    super(code);
  }
   
  @Override
  public boolean call(T v1) throws Exception {
    synchronized(this) {
      List<Object> stackInput = new ArrayList<Object>();
      stackInput.add(SparkUtils.fromSpark(v1));
      List<Object> stackResult = executor.exec(stackInput);

      if (1 == stackResult.size()) {
        return (boolean) SparkUtils.toSpark(stackResult.get(0));        
      } else {
        throw new RuntimeException("Filter code is expected to return a single boolean.");
      }
    }
  }
}