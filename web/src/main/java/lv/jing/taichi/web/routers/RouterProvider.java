package lv.jing.taichi.web.routers;

import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;
import java.util.Arrays;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * @author jing.lv
 */
@Singleton
public class RouterProvider implements Provider<Router> {

  private final Router router;

  @Inject
  public RouterProvider(
    Vertx vertx,
    @Named("cors") RouteBinder cors,
    @Named("auth") RouteBinder auth
  ) {
    this.router = Router.router(vertx);
    Arrays.asList(cors, auth).forEach(b -> b.route(this.router));
  }

  @Override
  public Router get() {
    return this.router;
  }

}
