package lv.jing.taichi.bitmap;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.Data;

/**
 * Bitmap group may name a group of bitmaps to ultilize the operations
 * 
 * @author jing.lv
 *
 */
@Data
public class BitmapGroup {
  
  String name;
  Set<String> biznames = new HashSet<String>();
  
  public BitmapGroup(String name) {
    this.name = name;
  }
  
  public int getSize() {
    return biznames.size();
  }
  
  public boolean add(String bizname) {
    biznames.add(bizname);
    return true;
  }
  
  public boolean add(String[] bizname) {
    for (int i = 0; i < bizname.length; i++) {
      biznames.add(bizname[i]);
    }
    return true;
  }
  
  public boolean remove(String bizname) {
    return biznames.remove(bizname);
  }
  
  public String[] getBiznames() {
    return biznames.stream().toArray(String[]::new);
  }
  
  public Set<String> getNameList() {
    return biznames;
  }

}
