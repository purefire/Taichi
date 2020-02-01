package lv.jing.taichi.statistics;

import com.google.common.collect.Table;

public class CounterManager {
  static Table<String, Long, Counter> counters;
  static Table<String, Long, Majority<Object>> majorities;

  public Counter getCounter(String bizname, long datetime) {
    Counter c = counters.get(bizname, datetime);
    if (c == null) {
      c = new Counter(bizname, datetime);
      counters.put(bizname, datetime, c);
    }
    return c;
  }

  public long increase(String bizname, long datetime, long value) {
    Counter c = getCounter(bizname, datetime);
    return c.increase(value);
  }

  public long increase(String bizname, String datetime, long value) {
    return increase(bizname, Long.valueOf(datetime), value);
  }

  public long increase(String bizname, long datetime) {
    return increase(bizname, datetime, 1L);
  }

  public double rate(String bizname1, long datetime1, String bizname2, long datetime2) {
    Counter c1 = getCounter(bizname1, datetime1);
    Counter c2 = getCounter(bizname2, datetime2);
    return ((Long) c1.get()).doubleValue()/ c2.get();
  }

  public Majority<Object> getMajority(String bizname, long datetime) {
    Majority<Object> c = majorities.get(bizname, datetime);
    if (c == null) {
      c = new Majority<Object>(bizname, datetime);
      majorities.put(bizname, datetime, c);
    }
    return c;
  }
  
  public boolean processMajority(String bizname, long datetime, Object value) {
    Majority<Object> c = majorities.get(bizname, datetime);
    return c.input(value);
  }
  
  public Object ifMajority(String bizname, long datetime) {
    Majority<Object> c = majorities.get(bizname, datetime);
    return c.getMajority();
  }

}
