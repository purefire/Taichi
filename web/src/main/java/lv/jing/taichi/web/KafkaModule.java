package lv.jing.taichi.web;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import lv.jing.taichi.bitmap.BitMapManager;
import lv.jing.taichi.core.conf.KafkaConf;
import lv.jing.taichi.core.conf.TaichiConf;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;

/**
 * Provider for kafka client
 *
 * @author lvjing
 * @since 2019-04-17
 */
@Singleton
public class KafkaModule extends AbstractModule {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final Logger accesslog = LoggerFactory.getLogger("access");

  @Inject
  private BitMapManager bitMapManager;

  private KafkaConsumer<JsonObject, JsonObject> consumer;
  private final KafkaConf c;

  @Inject
  public KafkaModule(Vertx v, TaichiConf conf) {
    c = conf.getKafka();
    consumer = KafkaConsumer.create(v, c.getConfMap());
    log.info("===== connect kafka success! =====");
  }

  public void configure() {
  }

  public KafkaConsumer<JsonObject, JsonObject> get() {
    return this.consumer;
  }

  public void stopListener() {
    if (consumer != null) {
      consumer.close();
    }
  }

  public void startListener() {
    // subscribe to several topics
    Set<String> topics = new HashSet<>();
    topics.add(c.getTopic());
    consumer.subscribe(topics);
    // start kakfa consumer
    consumer.handler(record -> {
      try {
        JsonObject json = record.value();
        String biz = json.getString("bizname");
        String dt = json.getString("dt");
        if (json.containsKey("uid") && !json.getValue("uid").equals("null")) {
          Object uid = json.getValue("uid");
          if (uid instanceof String) {
            if (uid.equals("null")) {
              log.error("Kafka parsing a null message =" + record.toString());
              return;
            }
            bitMapManager.set(biz, dt, Long.valueOf((String) uid));
          } else {
            long luid;
            if (uid instanceof Integer) {
              luid = (int) uid;
            } else {
              luid = (long) uid;
            }
            bitMapManager.set(biz, dt, luid);
          }
        } else if (json.containsKey("deviceId")) {
          String uid = json.getString("deviceId");
          bitMapManager.strSet(biz, dt, uid);
        }
        accesslog.info("Kafka process: " + json.getString("bizname") + ",value=" + json.getString("dt") + ",value="
            + json.getValue("uid") + ",partition=" + record.partition() + ",offset=" + record.offset());
      } catch (Exception e) {
        e.printStackTrace();
        log.error("Kafka parsing with " + e + ", message =" + record.toString());
      }
    });
  }

}
