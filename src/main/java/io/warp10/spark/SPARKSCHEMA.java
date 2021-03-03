//
//   Copyright 2021  SenX S.A.S.
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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class SPARKSCHEMA extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private enum Target {
    JAVA,
    SCALA,
    PYTHON,
    DDL,
  }
  public SPARKSCHEMA(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    Target target = Target.valueOf(String.valueOf(top).toUpperCase());

    top = stack.pop();

    stack.push(schema(target, top, true));

    return stack;
  }

  private static final String schema(Target target, Object o) throws WarpScriptException {
    return schema(target, o, false);
  }

  public static final String schema(Target target, Object o, boolean outer) throws WarpScriptException {
    if (o instanceof Row) {
      Row row = (Row) o;
      StructType schema = row.schema();

      StringBuilder sb = new StringBuilder();
      switch (target) {
        case JAVA:
          sb.append("DataTypes.createStructType(new SructField[] {");
          break;
        case SCALA:
          sb.append("StructType(List(");
          break;
        case PYTHON:
          sb.append("StructType([");
          break;
        case DDL:
          if (!outer) {
            sb.append("STRUCT<");
          }
          break;
      }
      sb.append("\n");
      for (int i = 0; i < row.size(); i++) {
        if (0 != i) {
          sb.append(",\n");
        }
        String name = "c" + i;

        if (null != schema) {
          name = schema.apply(i).name();
        }
        switch (target) {
          case JAVA:
            sb.append("DataTypes.createStructField(\"" + name.replaceAll("\"", "\\\"") + "\",");
            sb.append(schema(target, row.get(i)));
            sb.append(",true)");
            break;
          case SCALA:
            sb.append("StructField(\"" + name.replaceAll("\"", "\\\"") + "\",");
            sb.append(schema(target, row.get(i)));
            sb.append(",true)");
            break;
          case PYTHON:
            sb.append("StructField(\"" + name.replaceAll("\"", "\\\"") + "\",");
            sb.append(schema(target, row.get(i)));
            sb.append(",True)");
            break;
          case DDL:
            if (outer) {
              sb.append("`" + name.replaceAll("`", "``") + "` ");
            } else {
              sb.append("'" + name.replaceAll("\'", "\\\'") + "': ");
            }
            sb.append(schema(target, row.get(i)));
            break;
        }
      }
      switch (target) {
        case JAVA:
          sb.append("\n");
          sb.append("})");
          break;
        case SCALA:
          sb.append("\n");
          sb.append("))");
          break;
        case PYTHON:
          sb.append("\n");
          sb.append("])");
          break;
        case DDL:
          if (!outer) {
            sb.append(">");
          }
          break;
      }
      return sb.toString();
    } else if (o instanceof List) {
      StringBuilder sb = new StringBuilder();
      switch (target) {
        case JAVA:
          sb.append("DataTypes.createArrayType(");
          sb.append(schema(target, ((List) o).get(0)));
          sb.append(",true)");
          break;
        case SCALA:
          sb.append("ArrayType(");
          sb.append(schema(target, ((List) o).get(0)));
          sb.append(",true)");
          break;
        case PYTHON:
          sb.append("ArrayType(");
          sb.append(schema(target, ((List) o).get(0)));
          sb.append(",True)");
          break;
        case DDL:
          sb.append("ARRAY<");
          sb.append(schema(target, ((List) o).get(0)));
          sb.append(">");
          break;
      }
      return sb.toString();
    } else if (o instanceof Map) {
      StringBuilder sb = new StringBuilder();
      switch (target) {
        case JAVA:
          sb.append("DataTypes.createMapType(");
          Entry<Object,Object> entry = ((Map<Object,Object>) o).entrySet().iterator().next();
          sb.append(schema(target, entry.getKey()));
          sb.append(",");
          sb.append(schema(target, entry.getValue()));
          sb.append(",true)");
          break;
        case SCALA:
          sb.append("MapType(");
          entry = ((Map<Object,Object>) o).entrySet().iterator().next();
          sb.append(schema(target, entry.getKey()));
          sb.append(",");
          sb.append(schema(target, entry.getValue()));
          sb.append(",true)");
          break;
        case PYTHON:
          sb.append("MapType(");
          entry = ((Map<Object,Object>) o).entrySet().iterator().next();
          sb.append(schema(target, entry.getKey()));
          sb.append(",");
          sb.append(schema(target, entry.getValue()));
          sb.append(",True)");
          break;
        case DDL:
          sb.append("MAP<");
          entry = ((Map<Object,Object>) o).entrySet().iterator().next();
          sb.append(schema(target, entry.getKey()));
          sb.append(",");
          sb.append(schema(target, entry.getValue()));
          sb.append(">");
          break;
      }
      return sb.toString();
    } else if (o instanceof String) {
      switch (target) {
        case JAVA:
          return "DataTypes.StringType";
        case SCALA:
          return "StringType";
        case PYTHON:
          return "StringType()";
        case DDL:
          return "STRING";
      }
    } else if (o instanceof Double) {
      switch (target) {
        case JAVA:
          return "DataTypes.DoubleType";
        case SCALA:
          return "DoubleType";
        case PYTHON:
          return "DoubleType()";
        case DDL:
          return "DOUBLE";
      }
    } else if (o instanceof Long) {
      switch (target) {
        case JAVA:
          return "DataTypes.LongType";
        case SCALA:
          return "LongType";
        case PYTHON:
          return "LongType()";
        case DDL:
          return "LONG";
      }
    } else if (o instanceof Boolean) {
      switch (target) {
        case JAVA:
          return "DataTypes.BooleanType";
        case SCALA:
          return "BooleanType";
        case PYTHON:
          return "BooleanType()";
        case DDL:
          return "BOOLEAN";
      }
    } else if (o instanceof byte[]) {
      switch (target) {
        case JAVA:
          return "DataTypes.BinaryType";
        case SCALA:
          return "BinaryType";
        case PYTHON:
          return "BinaryType()";
        case DDL:
          return "BINARY";
      }
    } else if (null == o) {
      switch (target) {
        case JAVA:
          return "DataTypes.NullType";
        case SCALA:
          return "NullType";
        case PYTHON:
          return "NullType()";
        case DDL:
          return "OBJECT";
      }
    } else {
      throw new WarpScriptException("Unsupported type " + o.getClass());
    }
    return null;
  }
}
