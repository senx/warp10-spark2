//
//   Copyright 2018-2022  SenX S.A.S.
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

      if (1 == stackResult.size()) {
        return (R) SparkUtils.toSpark(stackResult.get(0));
      } else {
        return (R) SparkUtils.toSpark(stackResult);
      }
    }
  }

  public static WarpScriptFunction getInstance(String code) throws WarpScriptException {
    return new WarpScriptFunction(code);
  }
}
