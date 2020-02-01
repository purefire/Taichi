package lv.jing.taichi.core.conf;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;

/**
 * Kafka Conf
 *
 * @author lvjing
 * @since 2019-04-17
 */
@Data
public class KafkaConf {
  private String server;
  private String keyDeserializer;
  private String valueDeserializer;
  private String groupId;
  private String offsetReset;
  private String autoCommint;
  private String topic;
  
  private HashMap<String, String> map;
  
  public Map<String, String> getConfMap(){
    if (map==null) {
      map = new HashMap<String, String>();
      map.put("bootstrap.servers", server);
      map.put("key.deserializer", keyDeserializer);
      map.put("value.deserializer", valueDeserializer);
      map.put("group.id", groupId);
      map.put("auto.offset.reset", offsetReset);
      map.put("enable.auto.commit", autoCommint);
    }
    return map;
  }

}
