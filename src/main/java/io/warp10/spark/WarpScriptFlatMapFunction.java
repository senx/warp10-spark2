package io.warp10.spark2;

import io.warp10.script.WarpScriptException;
import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WarpScriptFlatMapFunction<T, R> extends WarpScriptAbstractFunction implements FlatMapFunction<T, R> {

  public WarpScriptFlatMapFunction(String code) throws WarpScriptException {
    super(code);
  }

  @Override
  public Iterator<R> call(T t) throws Exception {
    synchronized(this) {
      List<Object> stackInput = new ArrayList<Object>();
      stackInput.add(SparkUtils.fromSpark(t));
      List<Object> stackResult = executor.exec(stackInput);

      return ((Iterable<R>) SparkUtils.toSpark(stackResult)).iterator();
    }
  }
}
