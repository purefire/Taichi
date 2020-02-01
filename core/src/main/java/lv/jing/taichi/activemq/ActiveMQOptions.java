package lv.jing.taichi.activemq;

import java.util.UUID;

import javax.jms.Session;

import lombok.Data;

@Data
public class ActiveMQOptions {

  private String clientId = getUUID();
  private String broker = "tcp://127.0.0.1:61616";
  private String username = "admin";
  private String password = "admin";
  private int retryTimes = 5;
  private boolean transacted = false;
  private int acknowledgeMode = Session.AUTO_ACKNOWLEDGE;

  private String getUUID() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
