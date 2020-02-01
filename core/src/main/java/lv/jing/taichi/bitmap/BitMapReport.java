package lv.jing.taichi.bitmap;

import java.util.HashMap;

import lombok.Data;

@Data
public class BitMapReport {

    public int bitmapCount;
    public long heapSize;
    public HashMap<String, String> bitmaps;

}
