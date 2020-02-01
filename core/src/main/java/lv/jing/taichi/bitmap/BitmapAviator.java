package lv.jing.taichi.bitmap;

import java.util.Map;

import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorType;

public class BitmapAviator extends AviatorObject {
  private Bitmap value;
  
  public BitmapAviator(Bitmap value) {
    this.value = value;
  }

  @Override
  public int compare(AviatorObject other, Map<String, Object> env) {
    if (other instanceof BitmapAviator) {
      Bitmap otherbm = (Bitmap) other.getValue(env);
      if ((otherbm.getBizname().equals(this.value.getBizname())) && otherbm.getKeyname().equals(this.value.getKeyname()))
        return 0;
    }
    return -1;
  }

  @Override
  public AviatorType getAviatorType() {
    return AviatorType.JavaType;
  }

  @Override
  public Object getValue(Map<String, Object> env) {
    return value;
  }

}
