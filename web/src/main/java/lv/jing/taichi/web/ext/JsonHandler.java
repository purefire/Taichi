package lv.jing.taichi.web.ext;

import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.reactivex.functions.BiConsumer;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.http.HttpServerResponse;
import io.vertx.reactivex.ext.web.RoutingContext;
import java.lang.invoke.MethodHandles;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jing.lv
 */
public final class JsonHandler<T> implements BiConsumer<T, Throwable> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String APPLICATION_JSON = MediaType.JSON_UTF_8.toString();

  private final RoutingContext event;

  private JsonHandler(RoutingContext event) {
    this.event = event;
  }

  @Override
  public void accept(T o, Throwable e) throws Exception {
    if (this.event.response().closed()) {
      log.warn("response is closed: {} {}", this.event.request().method(),
        this.event.request().uri());
      return;
    }
    this.event.response().putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON);
    if (e != null) {
      this.withError(e);
    } else {
      this.withData(o);
    }
  }

  @SuppressWarnings("unchecked")
  private void withData(T o) {
    final HttpServerResponse res = this.event.response();
    final ResponseWrapper foo;
    if (o instanceof ResponseWrapper) {
      foo = (ResponseWrapper) o;
    } else if (o instanceof Optional) {
      foo = ResponseWrapper.of(((Optional) o).orElse(null));
    } else if (o instanceof com.google.common.base.Optional) {
      foo = ResponseWrapper.of(((com.google.common.base.Optional) o).orNull());
    } else {
      foo = ResponseWrapper.of(o);
    }
    res.end(Json.encode(foo));
  }

  private void withError(Throwable e) {
    int c = 1;
    int status = 500;
    if (e instanceof IllegalArgumentException) {
      status = 400;
    } else {
      log.error("handle err: ", e);
    }
    this.event.response()
      .setStatusCode(status)
      .end(Json.encode(ResponseError.of(c, e.getMessage())));
  }

  public static <T> JsonHandler<T> create(RoutingContext ctx) {
    return new JsonHandler<>(ctx);
  }

}
