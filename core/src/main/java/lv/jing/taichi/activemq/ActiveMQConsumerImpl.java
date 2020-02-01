package lv.jing.taichi.activemq;

import java.util.concurrent.atomic.AtomicReference;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.command.ActiveMQTextMessage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;

public class ActiveMQConsumerImpl implements ActiveMQConsumer {

  private final String key;
  private final Vertx vertx;
  private final Session session;
  private final String destination;
  private Queue queue;

  public ActiveMQConsumerImpl(String key, Vertx vertx, Session session, String destination) {
    this.key = key;
    this.vertx = vertx;
    this.session = session;
    this.destination = destination;
    try {
      this.queue = session.createQueue(destination);
    } catch (JMSException e) {
      throw new IllegalArgumentException("Error in creating activeMQ queue, name = " + queue);
    }
  }

  private final AtomicReference<Future<MessageConsumer>> consumerRef = new AtomicReference<>();

  @Override
  public String getKey() {
    return this.key;
  }

  @Override
  public void listen(Handler<AsyncResult<JsonObject>> messageHandler) {
    vertx.executeBlocking(res -> {
      try {
        Future<MessageConsumer> future = this.consumerRef.get();
        if (future == null) {
          Future<MessageConsumer> newFuture = Future.future();
          // check if consumed
          if (this.consumerRef.compareAndSet(null, newFuture)) {
            MessageConsumer newConsumer = session.createConsumer(queue);
            newFuture.complete(newConsumer);
            newConsumer.setMessageListener(message -> {
              try {
                String msg = ((ActiveMQTextMessage) message).getText();
                messageHandler.handle(Future.succeededFuture(new JsonObject(msg)));
              } catch (JMSException e) {
                messageHandler.handle(Future.failedFuture(e));
              }
            });
          } else {
            messageHandler.handle(Future.failedFuture(new IllegalStateException(
                "ActiveMQ may not start multiply times! key = " + this.key + ", queue = " + this.queue)));
          }
        } else {
          messageHandler.handle(Future.failedFuture(new IllegalStateException(
              "ActiveMQ may not start multiply times! key = " + this.key + ", queue = " + this.queue)));
        }
      } catch (Exception e) {
        messageHandler.handle(Future.failedFuture(e));
      }
    }, null);
  }

  @Override
  public void close() {
    Future<MessageConsumer> future = this.consumerRef.get();
    if (future != null) {
      if (this.consumerRef.compareAndSet(future, null)) {
        try {
          future.result().close();
        } catch (JMSException ignore) {
          // ignore
        }
      }
    }
  }

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
