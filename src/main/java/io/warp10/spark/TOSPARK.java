package io.warp10.spark;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.spark.common.SparkUtils;

public class TOSPARK extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOSPARK(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    stack.push(SparkUtils.toSpark(top));
    
    return stack;
  }  
}
