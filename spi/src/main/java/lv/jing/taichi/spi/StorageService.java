package lv.jing.taichi.spi;

import java.io.File;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

/**
 * StorageService
 *
 * @author jing.lv
 */
public interface StorageService {

  /**
   * upload
   *
   * @param key key
   * @param file local file
   */
  String upload(String key, File file);

  /**
   * upload
   *
   * @param key key
   * @param file local file
   * @param expires expires
   * @param timeUnit time unit
   * @return url
   */
  String upload(String key, File file, long expires, TimeUnit timeUnit);

  /**
   * upload
   *
   * @param key key
   * @param in in
   * @param expires expires
   * @param timeUnit time unit
   * @return url
   */
  String upload(String key, InputStream in, long expires, TimeUnit timeUnit);

  /**
   * fetch file
   *
   * @param key key
   * @return stream
   */
  InputStream fetch(String key);
  
  /**
   * download file to local
   * @param key
   * @param file
   * @return
   */
  boolean download(String key, File file);

}
