package lv.jing.taichi.web;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import lv.jing.taichi.bitmap.BitMapManager;
import lv.jing.taichi.bitmap.BitmapGroup;
import lv.jing.taichi.bitmap.OPERATOR;
import lv.jing.taichi.web.RPCBitmap.BitmapReply;
import lv.jing.taichi.web.RPCBitmap.BitmapRequest;
import lv.jing.taichi.web.ext.ResponseError;
import lv.jing.taichi.web.ext.ResponseWrapper;

import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.grpc.VertxServer;
import io.vertx.grpc.VertxServerBuilder;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.http.HttpServer;
import io.vertx.reactivex.ext.web.Route;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.handler.BodyHandler;

/**
 * @author jing.lv
 */
@Singleton
public class Server implements Runnable {

  private static final String CORS_ORIGIN = "Access-Control-Allow-Origin";
  private static final String CORS_METHODS = "Access-Control-Allow-Methods";
  private static final String CORS_HEADERS = "Access-Control-Allow-Headers";
  private static final String CORS_CREDENTIALS = "Access-Control-Allow-Credentials";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final Logger accesslog = LoggerFactory.getLogger("access");
  
  private static long maintainTimer;

  public static enum SERVERMODE {
    MASTER, READWRITE, READONLY
  };

  private static SERVERMODE mode = SERVERMODE.READWRITE;

  // default wait time
  private static int WAIT_MS = 10;

  @Inject
  private Vertx vertx;
  @Inject
  @Named("port")
  private int port;
  @Inject
  private BitMapManager bitMapManager;
  @Inject
  private Router router;
  @Inject
  private ActiveMQModule amq;
  @Inject
  private KafkaModule kafka;

  private List<Route> routes = new ArrayList<Route>();

  PostBitmapGrpc.PostBitmapImplBase rpcservice = new PostBitmapGrpc.PostBitmapImplBase() {
    @Override
    public void getBitmap(BitmapRequest request,
        io.grpc.stub.StreamObserver<lv.jing.taichi.web.RPCBitmap.BitmapReply> responseObserver) {
      List<ByteString> list = null;
      try {
        list = bitMapManager.offer(request.getBizname(), request.getDate());
      } catch (IOException e) {
        // do nothing
      }

      if (list != null) {
        BitmapReply.Builder builder = BitmapReply.newBuilder().setHighbits(list.size() - 1);
        for (int i = 0; i < list.size(); i++) {
          builder.putBitmaps(i, list.get(i));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
      } else {
        BitmapReply.Builder builder = BitmapReply.newBuilder().setHighbits(0);
        builder.putBitmaps(0, ByteString.copyFromUtf8("0"));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
      }
    }
  };

  public Server setMode(SERVERMODE m) {
    mode = m;
    return this;
  }

  @Override
  public void run() {
    HttpServerOptions options = new HttpServerOptions();
    // for exec, which may be a bit large
    options.setMaxInitialLineLength(1024 * 16).setMaxHeaderSize(1024 * 16);
    final HttpServer server = this.vertx.createHttpServer(options);
    router.route().handler(BodyHandler.create());
    
    routes.add(router.route("/bitmap/*").handler(routingContext -> {
      // log it
      accesslog.info(routingContext.request().remoteAddress() + " " + routingContext.request().absoluteURI());
      routingContext.next();
    }));

    routes.add(router.post("/bitmap/calc").blockingHandler(routingContext -> {
      String expression = routingContext.request().getParam("express");
      if (expression == null || expression.length() == 0) {
        JsonObject param = routingContext.getBodyAsJson();
        expression = param.getString("express");
      }
      try {
        Future<String> future = bitMapManager.express(expression);
        String count = future.get(10, TimeUnit.MILLISECONDS);
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseWrapper.of(count)));
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
      }
    }));

    // gracefully shutdown
    router.get("/bitmap/shutdown").blockingHandler(routingContext -> {
      if (mode == SERVERMODE.READONLY) {
        System.exit(0);
      }
      amq.stopListener();
      kafka.stopListener();
      // stop all routes
      final Route thisroute = routingContext.currentRoute();
      vertx.cancelTimer(maintainTimer);
      routes.forEach(new Consumer<Route>() {
        @Override
        public void accept(Route t) {
          if (!t.equals(thisroute)) {
            t.disable();
          }
        }
      });

      try {
        Future<Long> count = this.bitMapManager.shutdown();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseWrapper.of(count.get(50, TimeUnit.MILLISECONDS))));
        System.exit(0);
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e) {
        e.printStackTrace();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00002", "Internal Error!")));
      }
    });

    if (mode != SERVERMODE.READONLY) {
      enableQueues();
      enableWriteRoute();
    }

    enableOpRoute();
    enableReadRoute();

    maintainTimer = vertx.setPeriodic(DateUtils.MILLIS_PER_HOUR, event -> {
      this.bitMapManager.maintain();
    });

    server.requestHandler(router).rxListen(port).subscribe(s -> log.info("http server is listening on {}.", port),
        e -> log.error("http server start failed:", e));

    // start rpc server
    VertxServer rpcServer = VertxServerBuilder.forAddress(this.vertx.getDelegate(), "0.0.0.0", 9999)
        .addService(rpcservice).build();

    // Start is asynchronous
    try {
      rpcServer.start();
    } catch (IOException e2) {
      // TODO Auto-generated catch block
      e2.printStackTrace();
    }
  }

  private void enableOpRoute() {
    WebClient client = WebClient.create(vertx);
    // input external bitmap to combine together
    routes.add(router.get("/bitmap/combine/:biz/:date/:filename").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate = routingContext.request().getParam("date");
      String filename = routingContext.request().getParam("filename");

      try {
        long count = this.bitMapManager.combine(biz, sdate, filename);
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseWrapper.of(count)));
      } catch (Exception e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
      }
    }));

    routes.add(router.route("/bitmap/callback/:biz").handler(BodyHandler.create()));
    routes.add(router.route("/bitmap/callback/:biz").blockingHandler(routingContext -> {
      log.error("body=" + routingContext.getBodyAsString());
      JsonObject json = routingContext.getBodyAsJson();
      String biz = routingContext.request().getParam("biz");
      // { "type": "txt", "from": "userName", "msg": "accountName" }
      // String from = routingContext.request().getParam("from");
      log.error("biz =" + biz);
      if (json == null) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end("{\"status\": \"FAILED\",\"ts\":" + System.currentTimeMillis() + "}");
      } else {
        log.error(json.toString());
        String name = json.getString("userName");
        if (biz == null || name == null || name.length() == 0) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end("{\"status\": \"FAILED\",\"ts\":" + System.currentTimeMillis() + "}");
        } else {
          Date today = new Date();
          String sdate = BitMapManager.formatDateToStr(today);
          Date date;
          try {
            date = BitMapManager.parseStrToDate(sdate);
            long count = this.bitMapManager.count(biz, date);
            log.error("start to send to reliao" + "http://10.154.3.219:8080/acn-backend/message/sendToUser?user=" + name
                + "&msg=" + biz + "%20DAU:" + count);
            client.get(8080, "10.154.3.219",
                "/acn-backend/message/sendToUser?user=" + name + "&msg=" + biz + "%20DAU:" + count).send(ar -> {
                  if (!ar.succeeded()) {
                    log.error(ar.cause().getMessage());
                    log.error(ar.toString());
                  }
                  routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
                      .end("{\"status\": \"SUC\",\"ts\":" + System.currentTimeMillis() + "}");
                });
          } catch (ParseException e1) {
            routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
                .end("{\"status\": \"SUC\",\"ts\":" + System.currentTimeMillis() + "}");
          } catch (Exception e1) {
            routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
                .end("{\"status\": \"SUC\",\"ts\":" + System.currentTimeMillis() + "}");
          }
        }
      }
    }));

    routes.add(router.route("/bitmap/debug/:biz/:date").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate = routingContext.request().getParam("date");

      try {
        boolean exist = this.bitMapManager.debugInfile(biz, sdate, biz + sdate + ".debug");
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseWrapper.of(exist)));
      } catch (Exception e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
      }
    }));

    routes.add(router.route("/bitmap/debug/:biz/:date/:biz2/:date2/:op").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate = routingContext.request().getParam("date");
      String biz2 = routingContext.request().getParam("biz2");
      String sdate2 = routingContext.request().getParam("date2");
      String sop = routingContext.request().getParam("op");
      OPERATOR op = OPERATOR.and;
      switch (sop) {
      case "or":
        op = OPERATOR.or;
        break;
      case "xor":
        op = OPERATOR.xor;
      default:
        break;
      }

      try {
        boolean exist = this.bitMapManager.debugInfile(biz, sdate, biz2, sdate2, op,
            biz + sdate + "." + biz2 + sdate2 + ".debug");
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseWrapper.of(exist)));
      } catch (Exception e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
      }
    }));
    // force maintenance
    routes.add(router.get("/bitmap/maintain").blockingHandler(routingContext -> {
      this.bitMapManager.maintain();
      routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
          .end(Json.encode(ResponseWrapper.of(1)));
    }));

    routes.add(router.get("/bitmap/force").blockingHandler(routingContext -> {
      this.bitMapManager.forceSave();
      routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
          .end(Json.encode(ResponseWrapper.of(this.bitMapManager.status())));
    }));

    routes.add(router.get("/bitmap/status").blockingHandler(routingContext -> {
      routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
          .end(Json.encode(ResponseWrapper.of(this.bitMapManager.status())));
    }));
  }

  private void enableQueues() {
    // enable activeMQ listener
    amq.startListener();
    kafka.startListener();
  }

  private void enableWriteRoute() {
    routes.add(router.route("/bitmap/set/:biz/:date/:uid").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate = routingContext.request().getParam("date");
      String inuid = routingContext.request().getParam("uid");

      String[] uids = inuid.split(",");
      for (String uid : uids) {
        try {
          this.bitMapManager.set(biz, sdate, Long.parseLong(uid));
        } catch (NumberFormatException e1) {
          log.debug("error number:" + uid);
          continue;
        }
      }
      long count = this.bitMapManager.count(biz, sdate); 
      try {
        routingContext.response().end(Json.encode(ResponseWrapper.of(count)));
      } catch (java.lang.IllegalStateException e) {
        // client closed 
        // ignore
      }
    }));

    routes.add(router.route("/bitmap/strset/:biz/:date/:suid").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate = routingContext.request().getParam("date");
      String suid = routingContext.request().getParam("suid");

      String[] uids = suid.split(",");
      for (String uid : uids) {
        try {
          this.bitMapManager.strSet(biz, sdate, suid);
        } catch (NumberFormatException e1) {
          log.debug("error number:" + uid);
          continue;
        }
      }
      long count = this.bitMapManager.count(biz, sdate);
      routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
          .end(Json.encode(ResponseWrapper.of(count)));
    }));
  }

  private void enableReadRoute() {
    routes.add(router.get("/bitmap/rate/:biz/:date1/:date2").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate1 = routingContext.request().getParam("date1");
      String sdate2 = routingContext.request().getParam("date2");
      Future<Long> future = this.bitMapManager.opCount(biz, sdate1, sdate2, OPERATOR.and);
      long count = 0;
      try {
        long date1count = this.bitMapManager.count(biz, sdate1);
        if (0 == date1count || -1 == date1count) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
          log.debug("counting error on data 1 count,  and date1 =" + date1count);
        } else {
          count = future.get(WAIT_MS, TimeUnit.MILLISECONDS);
          if (-1 == count) {
            routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
            log.debug("counting error on & count,  and count =" + count);
          } else {
            double rate = count * 1.0 / date1count;
            log.debug("counting, and count = " + count + ", date1 =" + date1count);
            routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encode(ResponseWrapper.of(rate)));
          }
        }
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e) {
        e.printStackTrace();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00002", "Internal Error!")));
      }
    }));

    routes.add(router.get("/bitmap/retention/:biz/:date1").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate1 = routingContext.request().getParam("date1");

      List<Integer> days = new ArrayList<Integer>();
      // Default export yesterday, 3, 7, and 30 days retention
      days.add(1);
      days.add(3);
      days.add(7);
      days.add(30);
      Future<HashMap<Integer, Double>> future = this.bitMapManager.retention(biz, sdate1, days, false);

      try {
        if (future == null) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
        } else {
          HashMap<Integer, Double> result = future.get(WAIT_MS, TimeUnit.MILLISECONDS);
          if (null == result || result.size() == 0) {
            routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encode(ResponseError.of("B00002", "No date to offer.")));
          } else {
            routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encode(ResponseWrapper.of(result)));
          }
        }
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e) {
        e.printStackTrace();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00002", "Internal Error!")));
      }
    }));

    routes.add(router.get("/bitmap/retention/:biz/:date1/:days").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate1 = routingContext.request().getParam("date1");
      String inday = routingContext.request().getParam("days");
      String[] sdays = inday.split(",");

      List<Integer> days = new ArrayList<Integer>();
      for (int i = 0; i < sdays.length; i++) {
        days.add(Integer.valueOf(sdays[i]));
      }

      Future<HashMap<Integer, Double>> future = this.bitMapManager.retention(biz, sdate1, days, false);

      try {
        if (future == null) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
        } else {
          HashMap<Integer, Double> result = future.get(WAIT_MS, TimeUnit.MILLISECONDS);
          if (null == result || result.size() == 0) {
            routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encode(ResponseError.of("B00002", "No date to offer.")));
          } else {
            routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encode(ResponseWrapper.of(result)));
          }
        }
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e) {
        e.printStackTrace();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00002", "Internal Error!")));
      }
    }));

    routes.add(router.get("/bitmap/avgRetention/:biz/:date1/:days").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate1 = routingContext.request().getParam("date1");
      String inday = routingContext.request().getParam("days");
      String[] sdays = inday.split(",");

      List<Integer> days = new ArrayList<Integer>();
      for (int i = 0; i < sdays.length; i++) {
        days.add(Integer.valueOf(sdays[i]));
      }

      Future<HashMap<Integer, Double>> future = this.bitMapManager.retention(biz, sdate1, days, true);

      try {
        if (future == null) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
        } else {
          HashMap<Integer, Double> result = future.get(WAIT_MS, TimeUnit.MILLISECONDS);
          if (null == result || result.size() == 0) {
            routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encode(ResponseError.of("B00002", "No date to offer.")));
          } else {
            routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encode(ResponseWrapper.of(result)));
          }
        }
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e) {
        e.printStackTrace();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00002", "Internal Error!")));
      }
    }));
    
    routes.add(router.get("/bitmap/PV/:biz/:date1/:days").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate1 = routingContext.request().getParam("date1");
      String inday = routingContext.request().getParam("days");
      String[] sdays = inday.split(",");
      List<Integer> days = new ArrayList<Integer>();
      for (int i = 0; i < sdays.length; i++) {
        days.add(Integer.valueOf(sdays[i]));
      }
      HashMap<Integer, Long> result = new HashMap<Integer, Long>();
      for (String d : sdays) {
        result = bitMapManager.pv(biz, sdate1, days);
      }
      routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
          .end(Json.encode(ResponseWrapper.of(result)));
    }));

    routes.add(router.get("/bitmap/DAU/:biz/:date1/:days").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate1 = routingContext.request().getParam("date1");
      String inday = routingContext.request().getParam("days");
      String[] sdays = inday.split(",");

      List<Integer> days = new ArrayList<Integer>();
      for (int i = 0; i < sdays.length; i++) {
        days.add(Integer.valueOf(sdays[i]));
      }

      Future<HashMap<Integer, Double>> future = this.bitMapManager.DAU(biz, sdate1, days);

      try {
        if (future == null) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
        } else {
          HashMap<Integer, Double> result = future.get(WAIT_MS, TimeUnit.MILLISECONDS);
          if (null == result || result.size() == 0) {
            routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encode(ResponseError.of("B00002", "No date to offer.")));
          } else {
            // add average value
            final double[] total = new double[1];
            result.values().forEach(consumer -> {
              total[0] += consumer;
            });
            result.put(result.size(), total[0] / result.size());
            routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encode(ResponseWrapper.of(result)));
          }
        }
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e) {
        e.printStackTrace();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00002", "Internal Error!")));
      }
    }));

    routes.add(router.get("/bitmap/group/add/:groupname/:bizs").blockingHandler(routingContext -> {
      String groupname = routingContext.request().getParam("groupname");
      String bizs = routingContext.request().getParam("bizs");
      if (groupname == null || bizs == null || groupname.length() == 0 || bizs.length() == 0) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
      } else {
        String[] list = bizs.split(",");
        BitmapGroup bg = bitMapManager.addToGroup(groupname, list);
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseWrapper.of(bg.getSize())));
      }
    }));
    
    routes.add(router.get("/bitmap/group/remove/:groupname/:bizs").blockingHandler(routingContext -> {
      String groupname = routingContext.request().getParam("groupname");
      String bizs = routingContext.request().getParam("bizs");
      if (groupname == null || bizs == null || groupname.length() == 0 || bizs.length() == 0) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
      } else {
        String[] list = bizs.split(",");
        BitmapGroup bg = bitMapManager.removeFromGroup(groupname, list);
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseWrapper.of(bg.getSize())));
      }
    }));

    routes.add(router.get("/bitmap/group/addPrefix/:groupname/:prefix").blockingHandler(routingContext -> {
      String groupname = routingContext.request().getParam("groupname");
      String prefix = routingContext.request().getParam("prefix");
      if (groupname == null || prefix == null || groupname.length() == 0 || prefix.length() == 0) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
      } else {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseWrapper.of(bitMapManager.addToGroupWithPrefix(groupname, prefix).getSize())));
      }
    }));

    routes.add(router.get("/bitmap/group/list").blockingHandler(routingContext -> {
      routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
          .end(Json.encode(ResponseWrapper.of(bitMapManager.getGroups())));
    }));

    routes.add(router.get("/bitmap/group/list/:groupname").blockingHandler(routingContext -> {
      String groupname = routingContext.request().getParam("groupname");
      if (groupname == null || groupname.length() == 0) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
      } else {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseWrapper.of(bitMapManager.getGroup(groupname).getBiznames())));
      }
    }));

    routes.add(router.get("/bitmap/group/dau/:groupname/:date").blockingHandler(routingContext -> {
      String groupname = routingContext.request().getParam("groupname");
      String date = routingContext.request().getParam("date");
      if (groupname == null || groupname.length() == 0) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
      } else {
        HashMap<String, Long> map = null;
        try {
          map = bitMapManager.listGDAU(groupname, date).get();
        } catch (InterruptedException | ExecutionException e1) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
        }
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseWrapper.of(map)));
      }
    }));

    routes.add(router.get("/bitmap/group/:op/:groupname/:dates").blockingHandler(routingContext -> {
      String groupname = routingContext.request().getParam("groupname");
      String op = routingContext.request().getParam("op");
      String[] dates = routingContext.request().getParam("dates").split(",");

      Future<HashMap<Integer, Double>> future = bitMapManager.countGroup(groupname, dates, op);
      HashMap<Integer, Double> counts;
      try {
        counts = future.get(WAIT_MS, TimeUnit.MILLISECONDS);
        if (0 == counts.size()) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
          log.error("counting error on & count");
        } else {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseWrapper.of(counts)));
        }
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e) {
        e.printStackTrace();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00002", "Internal Error!")));
      }
    }));

    routes.add(router.get("/bitmap/groupXorCount/:groupname/:date").blockingHandler(routingContext -> {
      String groupname = routingContext.request().getParam("groupname");
      String date = routingContext.request().getParam("date");
      Future<Long> future = bitMapManager.countGroup(groupname, date, OPERATOR.xor);
      long count = 0;
      try {
        count = future.get(WAIT_MS, TimeUnit.MILLISECONDS);
        if (-1 == count) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
          log.error("counting error on & count,  and count =" + count);
        } else {
          log.debug("counting, and count = " + count);
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseWrapper.of(count)));
        }
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e) {
        e.printStackTrace();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00002", "Internal Error!")));
      }
    }));

    routes.add(router.get("/bitmap/groupAndCount/:groupname/:date").blockingHandler(routingContext -> {
      String groupname = routingContext.request().getParam("groupname");
      String date = routingContext.request().getParam("date");
      Future<Long> future = bitMapManager.countGroup(groupname, date, OPERATOR.and);
      long count = 0;
      try {
        count = future.get(WAIT_MS, TimeUnit.MILLISECONDS);
        if (-1 == count) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
          log.error("counting error on & count,  and count =" + count);
        } else {
          log.debug("counting, and count = " + count);
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseWrapper.of(count)));
        }
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e) {
        e.printStackTrace();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00002", "Internal Error!")));
      }
    }));

    routes.add(router.get("/bitmap/groupOrCount/:groupname/:date").blockingHandler(routingContext -> {
      String groupname = routingContext.request().getParam("groupname");
      String date = routingContext.request().getParam("date");
      Future<Long> future = bitMapManager.countGroup(groupname, date, OPERATOR.or);
      long count = 0;
      try {
        count = future.get(WAIT_MS, TimeUnit.MILLISECONDS);
        if (-1 == count) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
          log.error("counting error on & count,  and count =" + count);
        } else {
          log.debug("counting, and count = " + count);
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseWrapper.of(count)));
        }
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e) {
        e.printStackTrace();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00002", "Internal Error!")));
      }
    }));

    routes.add(router.get("/bitmap/andCount/:biz/:date1/:biz2/:date2").blockingHandler(routingContext -> {
      String biz1 = routingContext.request().getParam("biz");
      String biz2 = routingContext.request().getParam("biz2");
      String sdate1 = routingContext.request().getParam("date1");
      String sdate2 = routingContext.request().getParam("date2");
      Future<Long> future = this.bitMapManager.opCount(biz1, sdate1, biz2, sdate2, OPERATOR.and);
      long count = 0;
      try {
        count = future.get(WAIT_MS, TimeUnit.MILLISECONDS);
        if (-1 == count) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
          log.error("counting error on & count,  and count =" + count);
        } else {
          log.debug("counting, and count = " + count);
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseWrapper.of(count)));
        }
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e) {
        e.printStackTrace();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00002", "Internal Error!")));
      }
    }));

    routes.add(router.get("/bitmap/orCount/:biz/:date1/:biz2/:date2").blockingHandler(routingContext -> {
      String biz1 = routingContext.request().getParam("biz");
      String biz2 = routingContext.request().getParam("biz2");
      String sdate1 = routingContext.request().getParam("date1");
      String sdate2 = routingContext.request().getParam("date2");
      Future<Long> future = this.bitMapManager.opCount(biz1, sdate1, biz2, sdate2, OPERATOR.or);
      long count = 0;
      try {
        count = future.get(WAIT_MS, TimeUnit.MILLISECONDS);
        if (-1 == count) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
          log.error("counting error on & count,  and count =" + count);
        } else {
          log.debug("counting, and count = " + count);
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseWrapper.of(count)));
        }
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e) {
        e.printStackTrace();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00002", "Internal Error!")));
      }
    }));

    routes.add(router.get("/bitmap/xorCount/:biz/:date1/:biz2/:date2").blockingHandler(routingContext -> {
      String biz1 = routingContext.request().getParam("biz");
      String biz2 = routingContext.request().getParam("biz2");
      String sdate1 = routingContext.request().getParam("date1");
      String sdate2 = routingContext.request().getParam("date2");
      Future<Long> future = this.bitMapManager.opCount(biz1, sdate1, biz2, sdate2, OPERATOR.xor);
      long count = 0;
      try {
        count = future.get(WAIT_MS, TimeUnit.MILLISECONDS);
        if (-1 == count) {
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
          log.error("counting error on & count,  and count =" + count);
        } else {
          log.debug("counting, and count = " + count);
          routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encode(ResponseWrapper.of(count)));
        }
      } catch (InterruptedException | TimeoutException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("A00001", "In opeartion.")));
      } catch (ExecutionException e) {
        e.printStackTrace();
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00002", "Internal Error!")));
      }
    }));

    routes.add(router.route("/bitmap/get/:biz/:date/:uid").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate = routingContext.request().getParam("date");
      String uid = routingContext.request().getParam("uid");

      Date date;
      try {
        date = BitMapManager.parseStrToDate(sdate);
        boolean exist = this.bitMapManager.get(biz, date, Long.valueOf(uid));
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseWrapper.of(exist)));
      } catch (ParseException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
      }
    }));
    
    routes.add(router.route("/bitmap/getBits/:biz/:date").handler(BodyHandler.create()));
    routes.add(router.route("/bitmap/getBits/:biz/:date").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate = routingContext.request().getParam("date");
      String uids = routingContext.request().getParam("uids");

      Date date;
      try {
        date = BitMapManager.parseStrToDate(sdate);
        String[] ulist = uids.split(",");
        Set<String> ret = new HashSet<String>();
        for (String u : ulist) {
          if(!this.bitMapManager.get(biz, date, Long.valueOf(u))) {
            ret.add(u);
          }
        }
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseWrapper.of(ret)));
      } catch (ParseException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
      }
    }));

    routes.add(router.route("/bitmap/fix/:biz/:date").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate = routingContext.request().getParam("date");
      long count = this.bitMapManager.fix(biz, sdate);
      routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
          .end(Json.encode(ResponseWrapper.of(count)));
    }));

    routes.add(router.route("/bitmap/count/:biz/:date").blockingHandler(routingContext -> {
      String biz = routingContext.request().getParam("biz");
      String sdate = routingContext.request().getParam("date");

      Date date;
      try {
        date = BitMapManager.parseStrToDate(sdate);
        long count = this.bitMapManager.count(biz, date);
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseWrapper.of(count)));
      } catch (ParseException e1) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8")
            .end(Json.encode(ResponseError.of("B00001", "Parameter Error!")));
      }
    }));
  }
}
