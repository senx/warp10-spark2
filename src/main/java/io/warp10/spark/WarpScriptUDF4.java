package io.warp10.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.api.java.UDF4;

import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;

public class WarpScriptUDF4<T1, T2, T3, T4, R> extends WarpScriptAbstractFunction implements UDF4<T1, T2, T3, T4, R> {

  @Override
  public R call(T1 v1, T2 v2, T3 v3, T4 v4) throws Exception {
    synchronized(this) {
      setCode(v1.toString());
      
      List<Object> stackInput = new ArrayList<Object>();
      stackInput.add(SparkUtils.fromSpark(v2));
      stackInput.add(SparkUtils.fromSpark(v3));
      stackInput.add(SparkUtils.fromSpark(v4));
      List<Object> stackResult = executor.exec(stackInput);

      if (1 == stackResult.size()) {
        return (R) SparkUtils.toSpark(stackResult.get(0));        
      } else {
        return (R) SparkUtils.toSpark(stackResult);
      }
    }
  }
}
