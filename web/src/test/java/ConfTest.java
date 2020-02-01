import lv.jing.taichi.core.conf.TaichiConf;
import org.testng.annotations.Test;

/**
 * ConfTest
 *
 * @author jing.lv
 */
public class ConfTest {

  @Test
  public void test(){
    TaichiConf tc = TaichiConf.loadConfigClasspath("config.yml");
    assert tc != null;
  }

}
