package lv.jing.taichi.bitmap;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;

public class OrFunction extends BasicBitmapVariadicFunction {

  public OrFunction(BitMapManager bmm) {
    super(bmm);
  }

  static String name = "or";
  
  @Override
  public String getName() {
    return name;
  }
  
  @Override
  public AviatorObject variadicCall(Map<String, Object> env, AviatorObject... args) {
    Bitmap[] btms = parseBitmaps(env, args);
    if (btms == null) return null;

    Future<Bitmap> future = manager.rawOp(btms, OPERATOR.or);
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
