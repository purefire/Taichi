package lv.jing.taichi.web.routers.binder;

import lv.jing.taichi.web.routers.RouteBinder;
import io.reactivex.Single;
import io.reactivex.SingleOnSubscribe;
import io.reactivex.schedulers.Schedulers;
import io.vertx.reactivex.core.http.HttpServerRequest;
import io.vertx.reactivex.ext.web.Router;
import java.lang.invoke.MethodHandles;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO do implementation
 * @author jing.lv
 */
@Singleton
@Named("auth")
public class AuthRouteBinder implements RouteBinder {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void route(Router router) {
    router.route().handler(context -> {
      final HttpServerRequest req = context.request();
      if (!StringUtils.startsWithIgnoreCase(req.uri(), "/admin")) {
        context.next();
        return;
      }
      Single
        .create((SingleOnSubscribe<Boolean>) emitter -> {
          try {
            //TODO auth
            emitter.onSuccess(Boolean.TRUE);
          } catch (Throwable e) {
            emitter.onSuccess(Boolean.FALSE);
          }
        })
        .subscribeOn(Schedulers.io())
        .subscribe(ok -> {
          if (!ok) {
            context.response().setStatusCode(403).end();
          }
        });
    });
  }

}
