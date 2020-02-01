package lv.jing.taichi.core.conf;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import lv.jing.taichi.core.helper.YAML;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TaichiConf
 *
 * @author jing.lv
 */
public class TaichiConf {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private MongoConf mongo;
  private RedisConf redis;
  private HDFSConf hdfs;
  private AMQConf amq;
  private KafkaConf kafka;

  public KafkaConf getKafka() {
    return kafka;
  }

  public void setKafka(KafkaConf kafka) {
    this.kafka = kafka;
  }
  
  public AMQConf getAMQ() {
    return amq;
  }

  public void setAMQ(AMQConf amq) {
    this.amq = amq;
  }

  public RedisConf getRedis() {
    return redis;
  }

  public void setRedis(RedisConf redis) {
    this.redis = redis;
  }

  public MongoConf getMongo() {
    return mongo;
  }

  public void setMongo(MongoConf mongo) {
    this.mongo = mongo;
  }

  public void setHDFS(HDFSConf hdfs) {
    this.hdfs = hdfs;
  }

  public HDFSConf getHDFS() {
    return hdfs;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", TaichiConf.class.getSimpleName() + "[", "]")
      .add("mongo=" + mongo)
      .add("redis=" + redis)
      .add("hdfs=" + hdfs)
      .add("amq=" + amq)
      .add("kafka=" + kafka)
      .toString();
  }

  public static TaichiConf loadConfig(String cfg) {
    return loadConfig(Paths.get(cfg));
  }

  public static TaichiConf loadConfigClasspath(String cfg) {
    try (InputStream in = Thread.currentThread().getContextClassLoader()
      .getResourceAsStream(cfg)) {
      Preconditions.checkNotNull(in);
      return YAML.parse(in, TaichiConf.class);
    } catch (Throwable e) {
      log.error("load {} in classpath failed:", cfg, e);
      throw Throwables.propagate(e);
    }
  }

  private static TaichiConf loadConfig(Path path) {
    try {
      final String s = new String(Files.readAllBytes(path), Charsets.UTF_8);
      final TaichiConf ret = YAML.parse(s, TaichiConf.class);
      if (log.isInfoEnabled()) {
        log.info("===== load config success: {} =====", path);
      }
      return ret;
    } catch (IOException e) {
      log.error("load {} failed:", path, e);
      throw Throwables.propagate(e);
    }
  }

}
