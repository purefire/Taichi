package lv.jing.taichi.statistics;

public class Majority<T> {
  String bizname;
  long datetime;
  T majority;
  volatile long count;
  
  public Majority(String bizname, long datetime) {
    this.bizname = bizname;
    this.datetime = datetime;
  }

  public boolean input(T value) {
    if (count == 0) {
      majority = value;
      count = 1;
    } else if (value == majority) {
      count += 1;
    } else {
      count -= 1;
    }
    return count > 0;
  }

  public boolean hasMajority() {
    return count > 0;
  }

  public T getMajority() {
    if (count > 0)
      return this.majority;
    return null;
  }
}
