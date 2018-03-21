package io.warp10.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.api.java.UDF6;

import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;

public class WarpScriptUDF6<T1, T2, T3, T4, T5, T6, R> extends WarpScriptAbstractFunction implements UDF6<T1, T2, T3, T4, T5, T6, R> {

  @Override
  public R call(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6) throws Exception {
    synchronized(this) {
      setCode(v1.toString());
      
      List<Object> stackInput = new ArrayList<Object>();
      stackInput.add(SparkUtils.fromSpark(v2));
      stackInput.add(SparkUtils.fromSpark(v3));
      stackInput.add(SparkUtils.fromSpark(v4));
      stackInput.add(SparkUtils.fromSpark(v5));
      stackInput.add(SparkUtils.fromSpark(v6));
      List<Object> stackResult = executor.exec(stackInput);

      if (1 == stackResult.size()) {
        return (R) SparkUtils.toSpark(stackResult.get(0));        
      } else {
        return (R) SparkUtils.toSpark(stackResult);
      }
    }
  }
}
