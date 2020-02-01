package lv.jing.taichi.core.conf;

import com.google.common.base.Objects;

/**
 * @author jing.lv
 */
public class RedisConf {

  private String[] hosts;
  private int maxTotal = 128;
  private int maxIdle = 128;
  private boolean testOnBorrow;
  private boolean blockWhenExhausted = true;

  public void setHosts(String[] hosts) {
    this.hosts = hosts;
  }

  public int getMaxTotal() {
    return maxTotal;
  }

  public void setMaxTotal(int maxTotal) {
    this.maxTotal = maxTotal;
  }

  public int getMaxIdle() {
    return maxIdle;
  }

  public void setMaxIdle(int maxIdle) {
    this.maxIdle = maxIdle;
  }

  public boolean isTestOnBorrow() {
    return testOnBorrow;
  }

  public void setTestOnBorrow(boolean testOnBorrow) {
    this.testOnBorrow = testOnBorrow;
  }

  public boolean isBlockWhenExhausted() {
    return blockWhenExhausted;
  }

  public void setBlockWhenExhausted(boolean blockWhenExhausted) {
    this.blockWhenExhausted = blockWhenExhausted;
  }

  public String[] getHosts() {
    return hosts;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("hosts", hosts)
      .add("maxTotal", maxTotal)
      .add("maxIdle", maxIdle)
      .add("testOnBorrow", testOnBorrow)
      .add("blockWhenExhausted", blockWhenExhausted)
      .toString();
  }
}
