package lv.jing.taichi.activemq;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

public interface ActiveMQSubscriber {

  String getKey();

  void listen(Handler<AsyncResult<JsonObject>> messageHandler);

  void close();

  void close(Handler<AsyncResult<Void>> handler);

}
