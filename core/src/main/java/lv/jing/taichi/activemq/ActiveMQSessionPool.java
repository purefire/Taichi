package lv.jing.taichi.activemq;

import java.time.LocalDateTime;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public class ActiveMQSessionPool {

  private static final Logger logger = LoggerFactory.getLogger(ActiveMQSessionPool.class);

  private final ActiveMQConnectionFactory connectionFactory;
  private final AtomicReference<Connection> connection = new AtomicReference<>(null);
  private final ActiveMQOptions options;
  private AtomicReference<Session> session = new AtomicReference<>(null);
  private final int retryTimes;

  public ActiveMQSessionPool(ActiveMQOptions options) {

    this.options = options;
    this.retryTimes = options.getRetryTimes() < 1 ? 5 : options.getRetryTimes();
    this.connectionFactory = new ActiveMQConnectionFactory(options.getUsername(), options.getPassword(),
        options.getBroker());
    Connection connection;

    try {
      connection = connectionFactory.createConnection();
      connection.setClientID(
          options.getClientId() == null ? "vertx-activemq-client:" + LocalDateTime.now() : options.getClientId());
      connection.start();
      this.connection.set(connection);
      createSession();
      logger.info("ActiveMQ connected, connection = " + connection);
    } catch (JMSException e) {
      reConnect();
    }
  }

  public ActiveMQSessionPool(ActiveMQOptions options, Handler<AsyncResult<Void>> handler) {

    this.options = options;
    this.retryTimes = options.getRetryTimes() < 1 ? 5 : options.getRetryTimes();
    this.connectionFactory = new ActiveMQConnectionFactory(options.getUsername(), options.getPassword(),
        options.getBroker());
    Connection connection;

    try {
      connection = connectionFactory.createConnection();
      connection.setClientID(
          options.getClientId() == null ? "vertx-activemq-client:" + LocalDateTime.now() : options.getClientId());
      connection.start();
      this.connection.set(connection);
      createSession();
      logger.info("ActiveMQ connect, connection = " + connection);
      handler.handle(Future.succeededFuture());
    } catch (JMSException e) {
      handler.handle(Future.failedFuture(e));
    }
  }

  private void reConnect() {
    logger.warn("ActiveMQ connection failed, broker = " + options.getBroker() + ", retrying ...");
    Semaphore access = new Semaphore(this.retryTimes);
    while (true) {
      if (access.tryAcquire()) {
        try {
          Connection oldConnection = this.connection.get();
          Connection newConnection = this.connectionFactory.createConnection();
          newConnection.setClientID(
              options.getClientId() == null ? "vertx-activemq-client:" + LocalDateTime.now() : options.getClientId());
          newConnection.start();
          if (this.connection.compareAndSet(oldConnection, newConnection)) {
            // refresh session
            createSession();
          } else {
            newConnection.close();
          }
          break;
        } catch (JMSException e) {
          // ignore the exception and continue the loop
        }
      } else {
        throw new IllegalStateException("ActiveMQ connection failed, broker = " + options.getBroker());
      }
    }
  }

  public Session getSession() {
    if (((ActiveMQSession) this.session.get()).isClosed()) {
      reConnect();
    }
    return this.session.get();
  }

  private void createSession() {
    try {
      Session session = connection.get().createSession(options.isTransacted(), options.getAcknowledgeMode());
      this.session.set(session);
    } catch (JMSException e) {
      throw new IllegalArgumentException("ActiveMQ session creation failed, connection = " + connection);
    }
  }
}
