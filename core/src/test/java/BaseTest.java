import com.google.inject.Guice;
import com.google.inject.Injector;
import lv.jing.taichi.core.CoreModule;
import lv.jing.taichi.core.conf.TaichiConf;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * @author jing.lv
 */
public abstract class BaseTest {

  protected Injector injector;

  @BeforeClass
  public void prepare() {
    final TaichiConf conf = TaichiConf.loadConfigClasspath("config.yml");
    this.injector = Guice.createInjector(new CoreModule(conf));
  }

  @AfterClass
  public void teardown() {
  }

}
