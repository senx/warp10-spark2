//
//   Copyright 2022  SenX S.A.S.
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

import java.util.List;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.spark.common.SparkUtils;
import scala.Tuple2;

public class TOSPARKTUPLE2 extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TOSPARKTUPLE2(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof List) || 2 != ((List) top).size()) {
      throw new WarpScriptException(getName() + " operates on a list of two elements.");
    }

    List<Object> l = (List<Object>) top;

    Tuple2 tuple2 = Tuple2.apply(SparkUtils.toSpark(l.get(0)), SparkUtils.toSpark(l.get(1)));

    stack.push(tuple2);

    return stack;
  }
}
