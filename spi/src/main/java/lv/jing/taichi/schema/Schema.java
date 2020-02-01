package lv.jing.taichi.schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author jing.lv
 */
public final class Schema implements Serializable {

  private static final long serialVersionUID = 8459394718425992653L;

  private final String name;
  private final String team;
  private final List<String> tags;
  private final List<Column> columns;

  private Schema(String name, String team, List<String> tags, List<Column> columns) {
    this.name = name;
    this.team = team;
    this.tags = tags;
    this.columns = columns;
  }

  public String getTeam() {
    return team;
  }

  public final String getName() {
    return name;
  }

  public final List<Column> getColumns() {
    return columns;
  }

  public List<String> getTags() {
    return tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Schema schema = (Schema) o;
    return Objects.equals(name, schema.name) &&
      Objects.equals(team, schema.team) &&
      Objects.equals(tags, schema.tags) &&
      Objects.equals(columns, schema.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, team, tags, columns);
  }

  public static Builder builder(String team, String name) {
    return new Builder(team, name);
  }

  public static final class Builder {

    private static final Pattern PATTERN_NAME = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]{0,31}$");

    private final String name;
    private final String team;
    private final Map<String, Column> columns = Maps.newLinkedHashMap();
    private final Set<String> tags = Sets.newLinkedHashSet();

    private Builder(final String team, final String name) {
      Preconditions.checkNotNull(team, "invalid team name: name is null.");
      if (!PATTERN_NAME.matcher(team).matches()) {
        throw new IllegalArgumentException("invalid team name: " + PATTERN_NAME.pattern());
      }
      Preconditions.checkNotNull(name, "invalid schema name: name is null.");
      if (!PATTERN_NAME.matcher(name).matches()) {
        throw new IllegalArgumentException("invalid schema name: " + PATTERN_NAME.pattern());
      }
      this.team = team;
      this.name = name;
    }

    public final Builder column(String name, ColumnType type) {
      Preconditions.checkNotNull(name, "invalid column name: name is null.");
      Preconditions.checkNotNull(name, "invalid column type: type is null.");
      if (!PATTERN_NAME.matcher(name).matches()) {
        throw new IllegalArgumentException("invalid column name: " + PATTERN_NAME.pattern());
      }
      this.columns.put(name, new Column(name, type));
      return this;
    }

    public final Builder tag(String first, String... others) {
      this.addTag(first);
      Arrays.asList(others).forEach(this::addTag);
      return this;
    }

    public final Builder tag(Collection<String> tags) {
      if (tags == null) {
        return this;
      }
      tags.forEach(this::addTag);
      return this;
    }

    public final Schema build() {
      Preconditions.checkArgument(!this.columns.isEmpty(), "schema columns is empty.");
      final List<Column> sortedColumns = Lists.newArrayList(this.columns.values());
      sortedColumns
        .sort((o1, o2) -> String.CASE_INSENSITIVE_ORDER.compare(o1.getName(), o2.getName()));
      final List<Column> columns = ImmutableList.copyOf(sortedColumns);
      if (this.tags.isEmpty()) {
        return new Schema(this.name, this.team, Collections.emptyList(), columns);
      }
      return new Schema(this.name, this.team, ImmutableList.copyOf(this.tags), columns);
    }

    private void addTag(String tag) {
      if (!PATTERN_NAME.matcher(tag).matches()) {
        throw new IllegalArgumentException("invalid schema tag: " + tag + "!");
      }
      this.tags.add(tag);
    }

  }


}
