//
//   Copyright 2018  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
package io.warp10.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction2;

import io.warp10.script.WarpScriptException;
import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;

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
