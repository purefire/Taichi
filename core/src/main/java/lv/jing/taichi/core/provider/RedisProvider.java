package lv.jing.taichi.core.provider;

import lv.jing.taichi.core.conf.RedisConf;
import lv.jing.taichi.core.conf.TaichiConf;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author jing.lv
 */
@Singleton
public class RedisProvider implements Provider<JedisCluster>, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final JedisCluster jedisCluster;

  @Inject
  public RedisProvider(TaichiConf conf) {
    final RedisConf c = conf.getRedis();
    final Set<HostAndPort> seeds = Arrays.stream(c.getHosts())
      .map(HostAndPort::parseString)
      .collect(Collectors.toSet());
    final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setBlockWhenExhausted(c.isBlockWhenExhausted());
    jedisPoolConfig.setMaxTotal(c.getMaxTotal());
    jedisPoolConfig.setMaxIdle(c.getMaxIdle());
    jedisPoolConfig.setTestOnBorrow(c.isTestOnBorrow());
    this.jedisCluster = new JedisCluster(seeds, jedisPoolConfig);
    log.info("===== connect redis success! =====");
  }

  @Override
  public JedisCluster get() {
    return this.jedisCluster;
  }

  @Override
  public void close() throws Exception {
    this.jedisCluster.close();
  }
}
