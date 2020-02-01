package lv.jing.taichi.bitmap;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.aviator.runtime.function.AbstractVariadicFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;

public abstract class BasicBitmapVariadicFunction extends AbstractVariadicFunction {
  
  BitMapManager manager; 
  public BasicBitmapVariadicFunction(BitMapManager bmm) {
    this.manager = bmm;
  }
  
  static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  static String splitor = "@";

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
  
  protected Bitmap[] parseBitmaps(Map<String, Object> env, AviatorObject... args) {
    Bitmap[] btms = new Bitmap[args.length];
    for (int i = 0; i < args.length; i++) {
      if (args[i] == null || args[i].getValue(env) == null) {
        btms[i] = EmptyLocalBitmap.getInstance();
      } else if (args[i].getValue(env) instanceof Bitmap) {
        btms[i] = (Bitmap) args[i].getValue(env);
      } else {
        try {
          String param = FunctionUtils.getStringValue(args[i], env);
          String name = param.split(splitor)[0];
          String date = param.split(splitor)[1];
          if (manager.existGroup(name)) {
            // group has priority
            BitmapGroup bg = manager.getGroup(name);
            Future<Bitmap> future = manager.rawOpGroup(bg, date, OPERATOR.or);
            // wait till result got
            btms[i] = future.get();
          } else {
            btms[i] = manager.get(name, date);
          }
        } catch (Exception e) {
          log.error("Parsing error:" + e);
          return null;
        }
      }
    }
    return btms;
  }

}
