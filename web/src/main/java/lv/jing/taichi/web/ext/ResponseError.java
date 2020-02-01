package lv.jing.taichi.web.ext;

import org.apache.commons.lang3.StringUtils;

/**
 * @author jing.lv
 */
public final class ResponseError {

  private final String code;
  private final String msg;

  private ResponseError(String code, String msg) {
    this.code = code;
    this.msg = msg;
  }

  public String getCode() {
    return code;
  }

  public String getMsg() {
    return msg;
  }

  public static ResponseError of(int code, String msg) {
    return of("A" + StringUtils.leftPad(String.valueOf(code), 5, '0'), msg);
  }

  public static ResponseError of(String code, String msg) {
    return new ResponseError(code, msg);
  }

}
