package lv.jing.taichi.schema;

/**
 * @author jing.lv
 */
public enum ColumnType {

  INT(0), LONG(1), FLOAT(2), DOUBLE(3), STRING(4), BOOLEAN(5);

  private int v;

  ColumnType(int v) {
    this.v = v;
  }

  public int value() {
    return this.v;
  }

  public static ColumnType of(Integer v) {
    if (v == null) {
      throw new IllegalArgumentException("invalid column type: value is null.");
    }
    for (ColumnType it : ColumnType.values()) {
      if (it.v == v) {
        return it;
      }
    }
    throw new IllegalArgumentException("invalid column type: " + v);
  }

}
