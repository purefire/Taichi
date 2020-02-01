package lv.jing.taichi.bitmap;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorString;

/**
 * support both raw bitmap and String (format "name@date")
 * @author jing.lv
 *
 */
public class debugFunction extends BasicBitmapVariadicFunction {
  
  public debugFunction(BitMapManager bmm) {
    super(bmm);
  }

  static String name = "debug";

  @Override
  public String getName() {
    return name;
  }
  
  @Override
  public AviatorObject variadicCall(Map<String, Object> env, AviatorObject... args) {
    Bitmap[] btms = parseBitmaps(env, args);
    if (btms == null) return null;
    
    for (int i = 0; i < btms.length; i++) {
      manager.debugInfile(btms[i], "debug."+System.currentTimeMillis()+".data");
    }

    return new AviatorString(String.valueOf(btms.length));
  }

}
