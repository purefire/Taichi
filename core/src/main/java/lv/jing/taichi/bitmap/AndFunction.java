package lv.jing.taichi.bitmap;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;

/**
 * support both raw bitmap and String (format "name@date")
 * @author jing.lv
 *
 */
public class AndFunction extends BasicBitmapVariadicFunction {
  
  public AndFunction(BitMapManager bmm) {
    super(bmm);
  }

  static String name = "and";

  @Override
  public String getName() {
    return name;
  }
  
  @Override
  public AviatorObject variadicCall(Map<String, Object> env, AviatorObject... args) {
    Bitmap[] btms = parseBitmaps(env, args);
    if (btms == null) return null;

    Future<Bitmap> future = manager.rawOp(btms, OPERATOR.and);
    while (true) {
      try {
        Bitmap ret = future.get(100, TimeUnit.MILLISECONDS);
        return new BitmapAviator(ret);
      } catch (TimeoutException e) {
        // do noting
      } catch (InterruptedException | ExecutionException e) {
        return null;
      }
    }
  }

}
