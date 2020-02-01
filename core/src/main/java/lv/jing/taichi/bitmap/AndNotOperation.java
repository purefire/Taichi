package lv.jing.taichi.bitmap;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.googlecode.aviator.runtime.type.AviatorObject;

public class AndNotOperation extends BasicBitmapOperation {

  String name = "&^";

  public AndNotOperation(BitMapManager bmm) {
    super(bmm);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
    Bitmap btm1 = parseBitmap(env, arg1);
    Bitmap btm2 = parseBitmap(env, arg2);
    if (btm1 == null || btm2 == null)
      return null;

    Future<Bitmap> future = manager.rawOp(new Bitmap[] { btm1, btm2 }, OPERATOR.andnot);
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
