package lv.jing.taichi.activemq;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.command.ActiveMQTextMessage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;

public class ActiveMQProducerImpl implements ActiveMQProducer {

  private final String key;
  private final Vertx vertx;
  private final Session session;
  private final DestinationType destinationType;
  private final String destination;
  private MessageProducer producer;

  public ActiveMQProducerImpl(String key, Vertx vertx, Session session, DestinationType destinationType,
      String destination) {
    this.key = key;
    this.vertx = vertx;
    this.session = session;
    this.destinationType = destinationType;
    this.destination = destination;
    try {

      switch (destinationType) {
      case QUEUE:
        this.producer = session.createProducer(session.createQueue(destination));
        break;
      case TOPIC:
        this.producer = session.createProducer(session.createTopic(destination));
        break;
      }
    } catch (JMSException e) {
      throw new IllegalArgumentException("ActiveMQ producer creation failed, destination = " + destination);
    }

  }

  @Override
  public String getKey() {
    return this.key;
  }

  @Override
  public void send(JsonObject message) {
    this.send(message, null);
  }

  @Override
  public void send(JsonObject message, Handler<AsyncResult<Void>> handler) {
    vertx.executeBlocking(future -> {
      try {
        ActiveMQTextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(message.toString());
        this.producer.send(textMessage);
        future.complete();
      } catch (Exception e) {
        future.fail(e);
      }
    }, handler);
  }

  @Override
  public void close() {
    try {
      this.producer.close();
    } catch (JMSException ignore) {
      // ignore this exception
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
