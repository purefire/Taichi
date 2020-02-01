package lv.jing.taichi.bitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.ObjectUtils.Null;
import org.roaringbitmap.RoaringBitmap;

import com.google.protobuf.ByteString;

/**
 * An Null bit map is created as place holder or return a quick empty bit map
 * 
 * @author jing.lv
 *
 */
public class EmptyLocalBitmap extends LocalBitmap {
  private static EmptyLocalBitmap instance = new EmptyLocalBitmap();
  
  public static EmptyLocalBitmap getInstance() {
    return instance;
  }

  public EmptyLocalBitmap(String biz, String date, String filename) {
    super(biz, date, filename);
  }
  
  public EmptyLocalBitmap() {
    super(new RoaringBitmap());
  }

  @Override
  public BitMapType getType() {
    return BitMapType.LOCAL;
  }

  @Override
  public long count() {
    return 0L;
  }

  @Override
  public void set(long location) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exist(long location) {
    return false;
  }

  @Override
  public void set(int location) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exist(int location) {
    return false;
  }

  @Override
  public void clear(int location) {
    return;
  }

  @Override
  public void clear(long location) {
    return;
  }

  @Override
  public String getKeyname() {
    return "NULLBitmap";
  }

  @Override
  public String getBizname() {
    return "NULLBitmap";
  }

  @Override
  public void serialize(String filename) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Bitmap deSerialize(Set<Integer> highbits) {
    return this;
  }

  @Override
  public long lastUpdate() {
    return 0L;
  }

  @Override
  public long getByteSize() {
    return 0L;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public STATUS getStatus() {
    return STATUS.BOTH;
  }

  @Override
  public Set<Integer> highBits() {
    return new HashSet<Integer>();
  }

  @Override
  public long combile(Bitmap map) {
    return 0L;
  }

  @Override
  public long fix() {
    return 0L;
  }

  @Override
  public void set(String location) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exist(String location) {
    return false;
  }

  @Override
  public List<ByteString> serializeToBytes() throws IOException {
    return new ArrayList<>();
  }

  @Override
  public String getStorageName() {
    return "NULLBitmap";
  }

}
