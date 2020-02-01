import lv.jing.taichi.bitmap.BitMapManager;
import org.testng.annotations.Test;

/**
 * @author jing.lv
 */
public class BimapManagerTest extends BaseTest{

  @Test
  public void test(){
    BitMapManager manager = this.injector.getInstance(BitMapManager.class);
  }

}
