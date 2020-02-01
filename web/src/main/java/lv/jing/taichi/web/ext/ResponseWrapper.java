package lv.jing.taichi.web.ext;

/**
 * @author jing.lv
 */
public final class ResponseWrapper<T> {

  private static transient final String CODE_SUCCESS = "A00000";

  @SuppressWarnings("unchecked")
  private static final ResponseWrapper EMPTY = new ResponseWrapper(null);

  private final String code = CODE_SUCCESS;
  private final T data;

  private ResponseWrapper(T data) {
    this.data = data;
  }

  public String getCode() {
    return this.code;
  }

  public T getData() {
    return data;
  }

  public static <T> ResponseWrapper<T> of(T t) {
    if (t == null) {
      return empty();
    }
    return new ResponseWrapper<>(t);
  }

  @SuppressWarnings("unchecked")
  public static <T> ResponseWrapper<T> empty() {
    return EMPTY;
  }

}
