package io.warp10.spark;

import java.util.Arrays;

import org.apache.spark.sql.Row;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class ROWCOLS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public ROWCOLS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {    
    Object top = stack.pop();
    
    if (!(top instanceof Row)) {
      throw new WarpScriptException(getName() + " operates on a Row instance.");
    }
    
    Row row = (Row) top;
    
    stack.push(Arrays.asList(row.schema().fieldNames()));
    
    return stack;
  }
}
