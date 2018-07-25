package io.warp10.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import io.warp10.script.WarpScriptException;
import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;

public class WarpScriptMapFunction<T, R> extends WarpScriptAbstractFunction implements MapFunction<T, R> {

  public WarpScriptMapFunction(String code) throws WarpScriptException {
    super(code);
  }

  @Override
  public R call(T v1) throws Exception {
    synchronized(this) {
      List<Object> stackInput = new ArrayList<Object>();
      stackInput.add(SparkUtils.fromSpark(v1));
      List<Object> stackResult = executor.exec(stackInput);

      if (1 == stackResult.size()) {
        return (R) SparkUtils.toSpark(stackResult.get(0));        
      } else {
        return (R) SparkUtils.toSpark(stackResult);
      }
    }
  }
}