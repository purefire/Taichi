package lv.jing.taichi.statistics;

public class Counter {
  String bizname;
  String datetime;
  volatile long value;
  
  public Counter(String biz, long dt) {
    this.bizname = biz;
    this.datetime = String.valueOf(dt);
  }
  
  public Counter(String biz, String dt) {
    this.bizname = biz;
    this.datetime = dt;
  }

  public long increase(long v) {
    value += v;
    return value;
  }
  
  public long increase() {
    value ++;
    return value;
  }
  
  public long get() {
    return value;
  }
}
