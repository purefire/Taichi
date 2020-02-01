package lv.jing.taichi.web.routers.binder;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import lv.jing.taichi.web.routers.RouteBinder;
import io.vertx.reactivex.ext.web.Router;
import java.util.Map;
import javax.inject.Named;
import javax.inject.Singleton;

/**
 * @author jing.lv
 */
@Singleton
@Named("cors")
public class CORSRouteBinder implements RouteBinder {

  private static final Map<String, String> CORS_HEADERS;

  static {
    final String allowHeaders = Joiner.on(",").join(
      "origin",
      "content-type",
      "content-length",
      "accept",
      "x-requested-with",
      "access-control-allow-origin"
    );
    CORS_HEADERS = ImmutableMap.<String, String>builder()
      .put("Access-Control-Allow-Origin", "*")
      .put("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,OPTIONS,DELETE,HEAD")
      .put("Access-Control-Allow-Credentials", "true")
      .put("Access-Control-Allow-Headers", allowHeaders)
      .build();
  }

  @Override
  public void route(Router router) {
    // support CORS.
    router.route().handler(context -> {
      context.response().setChunked(true);
      CORS_HEADERS.forEach(context.response()::putHeader);
      context.next();
    });
    // skip request which method is OPTIONS.
    router.options().handler(ctx -> ctx.response().end());
  }

}
