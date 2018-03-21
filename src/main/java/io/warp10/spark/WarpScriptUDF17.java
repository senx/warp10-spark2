package io.warp10.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.api.java.UDF17;

import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;

public class WarpScriptUDF17<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, R> extends WarpScriptAbstractFunction implements UDF17<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, R> {

  @Override
  public R call(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6, T7 v7, T8 v8, T9 v9, T10 v10, T11 v11, T12 v12, T13 v13, T14 v14, T15 v15, T16 v16, T17 v17) throws Exception {
    synchronized(this) {
      setCode(v1.toString());
      
      List<Object> stackInput = new ArrayList<Object>();
      stackInput.add(SparkUtils.fromSpark(v2));
      stackInput.add(SparkUtils.fromSpark(v3));
      stackInput.add(SparkUtils.fromSpark(v4));
      stackInput.add(SparkUtils.fromSpark(v5));
      stackInput.add(SparkUtils.fromSpark(v6));
      stackInput.add(SparkUtils.fromSpark(v7));
      stackInput.add(SparkUtils.fromSpark(v8));
      stackInput.add(SparkUtils.fromSpark(v9));
      stackInput.add(SparkUtils.fromSpark(v10));
      stackInput.add(SparkUtils.fromSpark(v11));
      stackInput.add(SparkUtils.fromSpark(v12));
      stackInput.add(SparkUtils.fromSpark(v13));
      stackInput.add(SparkUtils.fromSpark(v14));
      stackInput.add(SparkUtils.fromSpark(v15));
      stackInput.add(SparkUtils.fromSpark(v16));
      stackInput.add(SparkUtils.fromSpark(v17));
      List<Object> stackResult = executor.exec(stackInput);

      if (1 == stackResult.size()) {
        return (R) SparkUtils.toSpark(stackResult.get(0));        
      } else {
        return (R) SparkUtils.toSpark(stackResult);
      }
    }
  }
}
