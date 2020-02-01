package lv.jing.taichi.core.conf;

import com.google.common.base.Objects;

/**
 * @author jing.lv
 */
public class MongoConf {

  private String[] hosts;
  private int port = 27017;
  private String database = "taichi";
  private String username;
  private String password;

  public String[] getHosts() {
    return hosts;
  }

  public void setHosts(String[] hosts) {
    this.hosts = hosts;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("hosts", hosts)
      .add("port", port)
      .add("database", database)
      .add("username", username)
      .add("password", password)
      .toString();
  }
}
