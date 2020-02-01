package lv.jing.taichi.activemq;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.reactivex.core.Vertx;

public class ActiveMQFactory{

  private final Vertx vertx;
  private final ActiveMQSessionPool sessionPool;
  
  public static ActiveMQFactory create(Vertx vertx2, ActiveMQOptions options) {
    return create(vertx2, options, null);
  }

  public static ActiveMQFactory create(Vertx vertx, ActiveMQOptions options, Handler<AsyncResult<Void>> handler) {
    if (options.getUsername() == null) {
      throw new IllegalArgumentException("username cannot be null");
    }
    if (options.getPassword() == null) {
      throw new IllegalArgumentException("password cannot be null");
    }
    if (options.getBroker() == null) {
      throw new IllegalArgumentException("broker cannot be null");
    }
    return new ActiveMQFactory(vertx, options, handler);
  }
  
  ActiveMQFactory(Vertx vertx, ActiveMQOptions options, Handler<AsyncResult<Void>> handler) {
    this.vertx = vertx;
    this.sessionPool = handler == null ? new ActiveMQSessionPool(options) : new ActiveMQSessionPool(options, handler);
    this.vertx.getOrCreateContext().put(ActiveMQFactory.class.getTypeName(), this);
  }

  public ActiveMQConsumer createConsumer(String key, String destination) {
    return new ActiveMQConsumerImpl(key, vertx, sessionPool.getSession(), destination);
  }

  public ActiveMQSubscriber createSubscriber(String key, String destination) {
    return new ActiveMQSubscriberImpl(key, vertx, sessionPool.getSession(), destination);
  }

  public ActiveMQProducer createProducer(String key, DestinationType destinationType, String destination) {
    return new ActiveMQProducerImpl(key, vertx, sessionPool.getSession(), destinationType, destination);
  }

}
