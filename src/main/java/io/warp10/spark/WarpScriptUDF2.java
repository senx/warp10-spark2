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
import java.util.List;

import org.apache.spark.sql.api.java.UDF2;

import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;

public class WarpScriptUDF2<T1, T2, R> extends WarpScriptAbstractFunction implements UDF2<T1, T2, R> {

  @Override
  public R call(T1 v1, T2 v2) throws Exception {
    synchronized(this) {
      setCode(v1.toString());
      
      List<Object> stackInput = new ArrayList<Object>();
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
