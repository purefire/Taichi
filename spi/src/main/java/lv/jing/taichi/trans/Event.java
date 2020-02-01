package lv.jing.taichi.trans;

import lv.jing.taichi.schema.Schema;
import java.lang.invoke.MethodHandles;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jing.lv
 */
public final class Event {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Schema schema;
  private final Map<String, Object> data;
  private long timestamp = System.currentTimeMillis();

  public Event(final Schema schema) {
    this.schema = schema;
    this.data = new HashMap<>(schema.getColumns().size());
  }

  public final Event put(final String column, final Object value) {
    if (column == null || value == null) {
      return this;
    }
    final Boolean ok = this.schema.getColumns()
      .stream()
      .filter(it -> it.getName().equals(column))
      .findFirst()
      .map(column1 -> {
        this.data.put(column, value);
        return Boolean.TRUE;
      })
      .orElse(Boolean.FALSE);
    if (!ok && log.isDebugEnabled()) {
      log.debug("put data failed: column {} doesn't exist!", column);
    }
    return this;
  }

  public final Event setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public final Event setTimestamp(Date date) {
    this.timestamp = date.getTime();
    return this;
  }

  public final Schema getSchema() {
    return schema;
  }

  public final Map<String, Object> getData() {
    return data;
  }

  public final long getTimestamp() {
    return timestamp;
  }
}
