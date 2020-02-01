package lv.jing.taichi.web;

import java.lang.invoke.MethodHandles;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import lv.jing.taichi.bitmap.BitMapManager;
import lv.jing.taichi.bitmap.CountFunction;
import lv.jing.taichi.core.CoreModule;
import lv.jing.taichi.core.conf.AMQConf;
import lv.jing.taichi.core.conf.TaichiConf;
import lv.jing.taichi.web.Server.SERVERMODE;

import io.vertx.reactivex.core.Vertx;

/**
 * Bootstrap
 *
 * @author jing.lv
 */
public final class Bootstrap {

  private static final String DEFAULT_CONFIG = "config.yml";
  private static final String DEFAULT_PORT = "8888";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static Injector injector;

  public static void main(String[] args) {
    final PosixParser parser = new PosixParser();
    final Options options = new Options().addOption("p", "port", true, "listen port. (default: " + DEFAULT_PORT + ")")
        .addOption("c", "config", true, "config path. (default: config.yml in classpath)")
        .addOption("h", "help", false, "print usage.")
        .addOption("m", "mode", true, "readonly mode, or full mode.");
    final HelpFormatter helpFormatter = new HelpFormatter();
    final CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      helpFormatter.printHelp("Taichi web", options);
      return;
    }
    if (cmd.hasOption("h")) {
      helpFormatter.printHelp("Taichi web", options);
      return;
    }
    final Integer port = Integer.valueOf(cmd.getOptionValue("p", DEFAULT_PORT));
    final String cfg = cmd.getOptionValue("c");
    final TaichiConf conf;
    if (StringUtils.isBlank(cfg)) {
      log.info("load classpath config: {}", DEFAULT_CONFIG);
      conf = TaichiConf.loadConfigClasspath(DEFAULT_CONFIG);
    } else if (StringUtils.startsWith(cfg, "classpath:")) {
      final String f = cfg.substring("classpath:".length());
      log.info("load classpath config: {}", f);
      conf = TaichiConf.loadConfigClasspath(f);
    } else {
      log.info("load config: {}", cfg);
      conf = TaichiConf.loadConfig(cfg);
    }
    final String mode = cmd.getOptionValue("m", "full");
    final Vertx vertx = Vertx.vertx();

    if (mode != null && (mode.equals("readonly") || mode.equals("r"))) {
      log.info("Running in readonly mode:" + mode);
      final Injector injector = Guice.createInjector(new CoreModule(conf), new WebModule(vertx, port));
      injector.getInstance(BitMapManager.class).init();
      injector.getInstance(Server.class).setMode(SERVERMODE.READONLY).run();
    } else {
      log.info("Running in full mode:" + mode);
      final Injector injector = Guice.createInjector(new CoreModule(conf), new WebModule(vertx, port),
          new ActiveMQModule(vertx, conf), new KafkaModule(vertx,conf));
      injector.getInstance(BitMapManager.class).init();
      injector.getInstance(ActiveMQModule.class).configure();
      injector.getInstance(Server.class).run();
    }
  }

}
