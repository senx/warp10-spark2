package io.warp10.spark;

import io.warp10.script.WarpScriptException;
import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

public class WarpScriptFunction2<T1, T2, R> extends WarpScriptAbstractFunction implements Function2<T1, T2, R> {

  public WarpScriptFunction2(String code) throws WarpScriptException {
    super(code);
  }

  @Override
  public R call(T1 v1, T2 v2) throws Exception {
    synchronized(this) {
      List<Object> stackInput = new ArrayList<Object>();
      stackInput.add(SparkUtils.fromSpark(v1));
      stackInput.add(SparkUtils.fromSpark(v2));
      List<Object> stackResult = executor.exec(stackInput);

      if (1 == stackResult.size()) {
        return (R) SparkUtils.toSpark(stackResult.get(0));
      } else {
        return (R) SparkUtils.toSpark(stackResult);
      }
    }
  }
}