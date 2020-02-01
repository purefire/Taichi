package lv.jing.taichi.schema;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author jing.;v
 */
public final class Column implements Serializable {

  private static final long serialVersionUID = -2551389174043853426L;

  private final String name;
  private final ColumnType type;

  Column(final String name, final ColumnType type) {
    this.name = name;
    this.type = type;
  }

  public final String getName() {
    return name;
  }

  public final ColumnType getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Column column = (Column) o;
    return Objects.equals(name, column.name) &&
      type == column.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }
}
