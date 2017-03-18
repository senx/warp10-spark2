package io.warp10.spark;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class SPARKTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SPARKTO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    stack.push(io.warp10.spark.common.SparkUtils.fromSpark(top));
    
    return stack;
  }  
}
