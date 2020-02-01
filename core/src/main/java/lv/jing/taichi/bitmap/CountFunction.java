package lv.jing.taichi.bitmap;

import java.util.Map;

import com.googlecode.aviator.runtime.function.AbstractVariadicFunction;
import com.googlecode.aviator.runtime.type.AviatorLong;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorType;

public class CountFunction extends BasicBitmapVariadicFunction {
  
  public CountFunction(BitMapManager bmm) {
    super(bmm);
  }

  private String name = "count";
  private static String splitor = "@";
  
  @Override
  public String getName() {
    return name;
  }

  @Override
  public AviatorObject variadicCall(Map<String, Object> env, AviatorObject... args) {
    if (args[0] != null) {
      if (args[0] instanceof BitmapAviator) {
        BitmapAviator ba = (BitmapAviator) args[0];
        return AviatorLong.valueOf(((Bitmap) ba.getValue(env)).count());
      } else {
        if (args[0].getAviatorType().equals(AviatorType.String)) {
          String strs = ((String) args[0].getValue(env));
          String bitmapName1 = strs.split(splitor)[0];
          String date1 = strs.split(splitor)[1];
          System.err.println(bitmapName1 + ":" + date1);
          Bitmap bm = manager.get(bitmapName1, date1);
          if (bm != null) {
            return AviatorLong.valueOf(bm.count());
          }
        }
      }
    }
    return AviatorLong.valueOf(-1L);
  }

}
