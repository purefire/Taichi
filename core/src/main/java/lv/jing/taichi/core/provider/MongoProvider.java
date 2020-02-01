package lv.jing.taichi.core.provider;

import lv.jing.taichi.core.conf.MongoConf;
import lv.jing.taichi.core.conf.TaichiConf;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jing.lv
 */
@Singleton
public class MongoProvider implements Provider<MongoDatabase> {

  private static final String AUTH_DATABASE = "admin";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final MongoClient client;
  private final MongoDatabase db;

  @Inject
  public MongoProvider(TaichiConf conf) {
    final MongoConf c = conf.getMongo();
    final List<ServerAddress> seeds = Arrays.stream(c.getHosts())
      .map(host -> new ServerAddress(host, c.getPort()))
      .collect(Collectors.toList());

    final MongoClientOptions options = MongoClientOptions.builder().build();
    if (StringUtils.isNotBlank(c.getUsername())) {
      final MongoCredential credential = MongoCredential.createCredential(
        c.getUsername(),
        AUTH_DATABASE,
        c.getPassword().toCharArray()
      );
      this.client = new MongoClient(seeds, credential, options);
    } else {
      this.client = new MongoClient(seeds, options);
    }
    this.db = client.getDatabase(conf.getMongo().getDatabase());
    log.info("connect mongodb success.");
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      this.client.close();
      log.info("disconnect mongodb success.");
    }));
  }

  @Override
  public MongoDatabase get() {
    return this.db;
  }

}
