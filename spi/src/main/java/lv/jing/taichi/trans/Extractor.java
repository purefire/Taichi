package lv.jing.taichi.trans;

import lv.jing.taichi.schema.Schema;
import java.util.Optional;

/**
 * @author jing.lv
 */
public interface Extractor {

  Optional<Event> extract(Schema schema, String content);

}
