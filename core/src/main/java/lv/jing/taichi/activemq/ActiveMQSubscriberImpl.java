package lv.jing.taichi.activemq;

import java.util.concurrent.atomic.AtomicReference;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.command.ActiveMQTextMessage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;

public class ActiveMQSubscriberImpl implements ActiveMQSubscriber {

  private final String key;
  private final Vertx vertx;
  private final Session session;
  private final String destination;
  private Topic topic;

  public ActiveMQSubscriberImpl(String key, Vertx vertx, Session session, String destination) {
    this.key = key;
    this.vertx = vertx;
    this.session = session;
    this.destination = destination;
    try {
      this.topic = session.createTopic(destination);
    } catch (JMSException e) {
      throw new IllegalArgumentException("ActiveMQ topic creation failed, topic = " + topic);
    }
  }

  private final AtomicReference<Future<TopicSubscriber>> subscriberRef = new AtomicReference<>();

  @Override
  public String getKey() {
    return this.key;
  }

  @Override
  public void listen(Handler<AsyncResult<JsonObject>> messageHandler) {
    this.vertx.executeBlocking(res -> {
      try {
        Future<TopicSubscriber> future = this.subscriberRef.get();
        if (future == null) {
          Future<TopicSubscriber> newFuture = Future.future();
          if (this.subscriberRef.compareAndSet(null, newFuture)) {
            TopicSubscriber newSubscriber = session.createDurableSubscriber(topic, this.key);
            newFuture.complete(newSubscriber);
            newSubscriber.setMessageListener(message -> {
              try {
                String msg = ((ActiveMQTextMessage) message).getText();
                messageHandler.handle(Future.succeededFuture(new JsonObject(msg)));
              } catch (JMSException e) {
                messageHandler.handle(Future.failedFuture(e));
              }
            });
          } else {
            messageHandler.handle(Future.failedFuture(new IllegalStateException(
                "ActiveMQ may not start multiple times! key = " + this.key + ", queue = " + this.topic)));
          }
        } else {
          messageHandler.handle(Future.failedFuture(new IllegalStateException(
              "ActiveMQ may not start multiple times! key = " + this.key + ", queue = " + this.topic)));
        }
      } catch (Exception e) {
        messageHandler.handle(Future.failedFuture(e));
      }
    }, null);
  }

  /**
   * 关闭topic监听
   */
  @Override
  public void close() {
    Future<TopicSubscriber> future = this.subscriberRef.get();
    if (future != null) {
      if (this.subscriberRef.compareAndSet(future, null)) {
        try {
          future.result().close();
        } catch (JMSException ignore) {
          // ignore this exception
        }
      }
    }
  }

  /**
   * 关闭topic监听
   */
  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    try {
      this.close();
      handler.handle(Future.succeededFuture());
    } catch (Exception e) {
      handler.handle(Future.failedFuture(e));
    }
  }
}
