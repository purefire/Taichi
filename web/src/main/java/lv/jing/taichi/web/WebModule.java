package lv.jing.taichi.web;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import javax.inject.Named;

import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import lv.jing.taichi.web.routers.RouteBinder;
import lv.jing.taichi.web.routers.RouterProvider;

import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;

/**
 * WebModule
 *
 * @author jing.lv
 */
public class WebModule extends AbstractModule {

  private int port;
  private Vertx vertx;

  public WebModule(Vertx vertx, int port) {
    this.vertx = vertx;
    this.port = port;
  }
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  protected void configure() {
    final Reflections reflections = new Reflections("lv.jing.taichi.web.routers.binder");
    reflections.getSubTypesOf(RouteBinder.class).forEach(clazz -> {
      final Named anno = clazz.getAnnotation(Named.class);
      if (anno == null) {
        return;
      }
      log.info("++++ new router found: {}", clazz.getSimpleName());
      this.bind(RouteBinder.class).annotatedWith(Names.named(anno.value())).to(clazz);
    });
    final Vertx vertx = Vertx.vertx();
    this.bind(Vertx.class).toInstance(vertx);
    this.bind(Router.class).toProvider(RouterProvider.class);
    this.bind(Server.class);
    //TODO authentication
    final Map<String, String> props = Maps.newHashMap();
    props.put("port", String.valueOf(this.port));
    Names.bindProperties(this.binder(), props);
  }

}
