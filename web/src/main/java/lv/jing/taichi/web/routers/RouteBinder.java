package lv.jing.taichi.web.routers;

import io.vertx.reactivex.ext.web.Router;

/**
 * @author jing.lv
 */
public interface RouteBinder {

  /**
   * bind router
   *
   * @param router router
   */
  void route(Router router);

}
