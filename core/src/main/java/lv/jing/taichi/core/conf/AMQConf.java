package lv.jing.taichi.core.conf;

import lombok.Data;

@Data
public class AMQConf {
  private String url;
  private String username;
  private String password;
  private String queue;

}
