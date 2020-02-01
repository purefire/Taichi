package lv.jing.taichi.core.helper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nonnull;

/**
 * @author jing.lv
 */
public final class YAML {

  private static final ObjectMapper OM;

  static {
    OM = new ObjectMapper(new YAMLFactory());
  }

  public static <T> T parse(@Nonnull String yaml, @Nonnull Class<T> clazz) {
    try {
      return OM.readValue(yaml, clazz);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> T parse(@Nonnull InputStream inputStream, @Nonnull Class<T> clazz) {
    try {
      return OM.readValue(inputStream, clazz);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static String stringify(Object object) {
    try (ByteArrayOutputStream bo = new ByteArrayOutputStream()) {
      OM.writeValue(bo, object);
      return new String(bo.toByteArray(), Charsets.UTF_8);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private YAML() {
  }
}
