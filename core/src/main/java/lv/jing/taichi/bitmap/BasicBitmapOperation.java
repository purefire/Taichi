package lv.jing.taichi.bitmap;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;

public abstract class BasicBitmapOperation extends AbstractFunction {

  BitMapManager manager;

  public BasicBitmapOperation(BitMapManager bmm) {
    this.manager = bmm;
  }

  static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static String splitor = "@";

  protected Bitmap parseBitmap(Map<String, Object> env, AviatorObject arg) {
    Bitmap btm = null;
    if (arg.getValue(env) instanceof Bitmap) {
      btm = (Bitmap) arg.getValue(env);
    } else {
      try {
        String param = FunctionUtils.getStringValue(arg, env);
        String name = param.split(splitor)[0];
        String date = param.split(splitor)[1];
        if (manager.existGroup(name)) {
          // group has priority 
          BitmapGroup bg = manager.getGroup(name);
          Future<Bitmap> future = manager.rawOpGroup(bg, date, OPERATOR.or);
          // wait till result got
          btm = future.get();
        } else {
          btm = manager.get(name, date);
        }
      } catch (Exception e) {
        log.error("Parsing error:" + e);
        return null;
      }
    }
    return btm;
  }

}
