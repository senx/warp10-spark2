package io.warp10.spark;

import io.warp10.script.WarpScriptException;
import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

public class WarpScriptFunction<T, R> extends WarpScriptAbstractFunction implements Function<T, R> {

  public WarpScriptFunction(String code) throws WarpScriptException {
    super(code);
  }

  @Override
  public R call(T v1) throws Exception {
    synchronized(this) {
      List<Object> stackInput = new ArrayList<Object>();
      stackInput.add(SparkUtils.fromSpark(v1));
      List<Object> stackResult = executor.exec(stackInput);

      return (R) SparkUtils.toSpark(stackResult);
    }
  }
}