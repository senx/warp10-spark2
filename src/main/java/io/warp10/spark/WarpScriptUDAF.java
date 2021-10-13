//
//   Copyright 2018-2021  SenX S.A.S.
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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import io.warp10.script.WarpScriptException;
import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;

public class WarpScriptUDAF extends UserDefinedAggregateFunction {

  private boolean deterministic = false;
  private StructType inputSchema = null;
  private StructType bufferSchema = null;
  private DataType dataType = null;

  private final WarpScriptAbstractFunction func;

  public WarpScriptUDAF(String mc2) {
    try {
      this.func = new WarpScriptAbstractFunction(mc2) {};
    } catch (WarpScriptException wse) {
      throw new RuntimeException(wse);
    }
  }

  @Override
  public void initialize(MutableAggregationBuffer buf) {
    try {
      List<Object> stackInput = new ArrayList<Object>();
      stackInput.add("initialize");
      stackInput.add(SparkUtils.fromSpark(buf));
      List<Object> stackOutput = this.func.getExecutor().exec(stackInput);

      if (stackOutput.size() != buf.size()) {
        throw new WarpScriptException("Invalid stack size in initialize, expecting " + buf.size() + " levels but got " + stackOutput.size());
      }

      for (int i = 0; i < buf.size(); i++) {
        buf.update(i, stackOutput.get(i));
      }
    } catch (WarpScriptException wse) {
      throw new RuntimeException(wse);
    }
  }

  @Override
  public void merge(MutableAggregationBuffer buf, Row row) {
    try {
      List<Object> stackInput = new ArrayList<Object>();
      stackInput.add("merge");
      stackInput.add(SparkUtils.fromSpark(row));
      stackInput.add(SparkUtils.fromSpark(buf));
      List<Object> stackOutput = this.func.getExecutor().exec(stackInput);

      if (stackOutput.size() != buf.size()) {
        throw new WarpScriptException("Invalid stack size in merge, expecting " + buf.size() + " levels but got " + stackOutput.size());
      }

      for (int i = 0; i < buf.size(); i++) {
        buf.update(i, stackOutput.get(i));
      }
    } catch (WarpScriptException wse) {
      throw new RuntimeException(wse);
    }
  }

  @Override
  public void update(MutableAggregationBuffer buf, Row row) {
    try {
      List<Object> stackInput = new ArrayList<Object>();
      stackInput.add("update");
      stackInput.add(SparkUtils.fromSpark(row));
      stackInput.add(SparkUtils.fromSpark(buf));
      List<Object> stackOutput = this.func.getExecutor().exec(stackInput);

      if (stackOutput.size() != buf.size()) {
        throw new WarpScriptException("Invalid stack size in update, expecting " + buf.size() + " levels but got " + stackOutput.size());
      }

      for (int i = 0; i < buf.size(); i++) {
        buf.update(i, stackOutput.get(i));
      }
    } catch (WarpScriptException wse) {
      throw new RuntimeException(wse);
    }
  }

  @Override
  public Object evaluate(Row row) {
    try {
      List<Object> stackInput = new ArrayList<Object>();
      stackInput.add("evaluate");
      stackInput.add(SparkUtils.fromSpark(row));

      List<Object> stackResult = this.func.getExecutor().exec(stackInput);

      if (1 == stackResult.size()) {
        return SparkUtils.toSpark(stackResult.get(0));
      } else {
        return SparkUtils.toSpark(stackResult);
      }
    } catch (WarpScriptException wse) {
      throw new RuntimeException(wse);
    }
  }

  @Override
  public boolean deterministic() {
    return this.deterministic;
  }

  @Override
  public StructType inputSchema() {
    return this.inputSchema;
  }

  @Override
  public StructType bufferSchema() {
    return this.bufferSchema;
  }

  @Override
  public DataType dataType() {
    return this.dataType;
  }

  public UserDefinedAggregateFunction setDeterministic(boolean deterministic) {
    this.deterministic = deterministic;
    return this;
  }

  public UserDefinedAggregateFunction setInputSchema(String inputSchema) {
    try {
      DataType type = DataType.fromJson(inputSchema);
      this.inputSchema = (StructType) type;
    } catch(IllegalArgumentException iae) {
      DataType type = LegacyTypeStringParser.parseString(inputSchema);
      this.inputSchema = (StructType) type;
    }
    return this;
  }

  public UserDefinedAggregateFunction setBufferSchema(String bufferSchema) {
    try {
      DataType type = DataType.fromJson(bufferSchema);
      this.bufferSchema = (StructType) type;
    } catch(IllegalArgumentException iae) {
      DataType type = LegacyTypeStringParser.parseString(bufferSchema);
      this.bufferSchema = (StructType) type;
    }
    return this;
  }

  public UserDefinedAggregateFunction setDataType(String dataType) {
    try {
      DataType type = DataType.fromJson(dataType);
      this.dataType = type;
    } catch(IllegalArgumentException iae) {
      DataType type = LegacyTypeStringParser.parseString(dataType);
      this.dataType = type;
    }
    return this;
  }
}
