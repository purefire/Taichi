package lv.jing.taichi.core.conf;

import lombok.Data;

@Data
public class HDFSConf {
  private String hdfsPath;
  private String yarnPath;
  private String corePath;
  private String mapredPath;

}
