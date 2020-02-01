package lv.jing.taichi.web;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import lv.jing.taichi.activemq.ActiveMQConsumer;
import lv.jing.taichi.activemq.ActiveMQFactory;
import lv.jing.taichi.activemq.ActiveMQOptions;
import lv.jing.taichi.bitmap.BitMapManager;
import lv.jing.taichi.core.conf.AMQConf;
import lv.jing.taichi.core.conf.TaichiConf;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;

/**
 * ActiveMQ Module
 *
 * @author lvjing
 */
public class ActiveMQModule extends AbstractModule {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final Logger accesslog = LoggerFactory.getLogger("access");

  private String url;
  private String username;
  private String password;
  private String queuename;
  private Vertx vertx;

  @Inject
  private BitMapManager bitMapManager;

  ActiveMQConsumer consumer;

  @Inject
  public ActiveMQModule(Vertx v, TaichiConf conf) {
    AMQConf aconf = conf.getAMQ();
    this.username = aconf.getUsername();
    this.password = aconf.getPassword();
    this.url = aconf.getUrl();
    this.queuename = aconf.getQueue();
    this.vertx = v;
  }

  @Override
  protected void configure() {
    final Map<String, String> props = Maps.newHashMap();
    props.put("amqurl", url);
    props.put("amquser", username);
    props.put("amqpass", password);
  }

  public void stopListener() {
    if (consumer != null) {
      consumer.close();
    }
  }

  public void startListener() {
    ActiveMQOptions options = new ActiveMQOptions();
    options.setUsername(username);
    options.setPassword(password);
    options.setBroker(url);

    ActiveMQFactory factory = ActiveMQFactory.create(vertx, options);
    consumer = factory.createConsumer("myKey", queuename);

    consumer.listen(res -> {
      if (res.succeeded()) {
        JsonObject message = res.result();
        switch (message.getString("op")) {
        case "maintain":
          this.bitMapManager.maintain();
          break;
        case "shutdown":
          // do not support for now
          // this.bitMapManager.shutdown();
          break;
        case "set":
        case "":
        default:
          String biz = message.getString("biz");
          String skey = message.getString("key");
          String inuid = message.getString("uid");
          if (biz == null || skey == null || inuid == null || biz.length() == 0 || skey.length() == 0) {
            break;
          }
          String[] uids = inuid.split(",");
          for (String uid : uids) {
            try {
              this.bitMapManager.set(biz, skey, Long.parseLong(uid));
            } catch (NumberFormatException e1) {
              log.debug("error number:" + uid);
              continue;
            }
          }
          break;
        }
        accesslog.info("MQ receive:" + res.result().toString());
      } else {
        res.cause().printStackTrace();
      }
    });
  }

}
