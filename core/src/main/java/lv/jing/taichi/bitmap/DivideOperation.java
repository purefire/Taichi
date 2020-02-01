package lv.jing.taichi.bitmap;

import java.util.Map;

import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorDouble;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorType;

public class DivideOperation extends BasicBitmapOperation {

  String name = "/";

  public DivideOperation(BitMapManager bmm) {
    super(bmm);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
    Double d1 = 0.0; 
    Double d2 = 0.0;
    if (arg1.getAviatorType() == AviatorType.Double || arg1.getAviatorType() == AviatorType.Long) {
      Number n1 = FunctionUtils.getNumberValue(arg1, env);
      d1 = n1.doubleValue();
    } else {
      Bitmap btm = parseBitmap(env, arg1);
      Long l = btm.count();
      d1 = l.doubleValue();
    }
    if (arg2.getAviatorType() == AviatorType.Double || arg2.getAviatorType() == AviatorType.Long) {
      Number n2 = FunctionUtils.getNumberValue(arg2, env);
      d1 = n2.doubleValue();
    } else {
      Bitmap btm = parseBitmap(env, arg2);
      Long l = btm.count();
      d2 = l.doubleValue();
    }
    if (d2 != 0)
      return AviatorDouble.valueOf(d1 / d2);
    else {
      return AviatorDouble.valueOf(-1);
    }
  }

}
