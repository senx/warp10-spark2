package io.warp10.spark;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class TOSPARKROW extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOSPARKROW(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a list.");
    }
    
    List<Object> l = (List<Object>) top;
    
    Object[] converted = new Object[l.size()];
    
    for (int i = 0; i < l.size(); i++) {
      converted[i] = SparkUtils.toSpark(l.get(i));
    }

    Row row = new GenericRow(converted);

    stack.push(row);
    
    return stack;
  }  
}
