package io.warp10.spark;

import io.warp10.script.WarpScriptException;
import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WarpScriptFlatMapFunction2<T1, T2, R> extends WarpScriptAbstractFunction implements FlatMapFunction2<T1, T2, R> {

  public WarpScriptFlatMapFunction2(String code) throws WarpScriptException {
    super(code);
  }

  @Override
  public Iterator<R> call(T1 t1, T2 t2) throws Exception {
    synchronized(this) {
      List<Object> stackInput = new ArrayList<Object>();
      stackInput.add(SparkUtils.fromSpark(t1));
      stackInput.add(SparkUtils.fromSpark(t2));
      List<Object> stackResult = executor.exec(stackInput);

      return ((Iterable<R>) SparkUtils.toSpark(stackResult)).iterator();
    }
  }
}
