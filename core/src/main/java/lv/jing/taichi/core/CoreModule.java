package lv.jing.taichi.core;

import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;
import lv.jing.taichi.anno.Trace;
import lv.jing.taichi.bitmap.BitMapManager;
import lv.jing.taichi.core.aop.TraceInterceptor;
import lv.jing.taichi.core.conf.TaichiConf;
import lv.jing.taichi.core.provider.MongoProvider;
import lv.jing.taichi.core.provider.RedisProvider;
import com.mongodb.client.MongoDatabase;

import redis.clients.jedis.JedisCluster;

/**
 * CoreModule
 *
 * @author jing.lv
 */
public class CoreModule extends AbstractModule {

  private final TaichiConf conf;

  public CoreModule(TaichiConf conf) {
    this.conf = conf;
  }

  @Override
  protected void configure() {
    this.bind(TaichiConf.class).toInstance(this.conf);
    this.bind(MongoDatabase.class).toProvider(MongoProvider.class);
    this.bind(JedisCluster.class).toProvider(RedisProvider.class);
    this.bind(BitMapManager.class);
    this.bindInterceptor(
      Matchers.any(),
      Matchers.annotatedWith(Trace.class),
      new TraceInterceptor()
    );
  }

}
