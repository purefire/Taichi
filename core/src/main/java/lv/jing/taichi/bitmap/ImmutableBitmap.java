package lv.jing.taichi.bitmap;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.protobuf.ByteString;

/**
 * An ummutable bit map is created from another map, but cannot be changed.
 * Typically it is used after bit map operation like "AND, OR, XOR" etc. to check
 * the result.
 * 
 * @author jing.lv
 *
 */
public class ImmutableBitmap implements Bitmap {

    Bitmap realMap;
    
    public ImmutableBitmap(Bitmap map) {
        this.realMap = map;
    }

    @Override
    public BitMapType getType() {
        return realMap.getType();
    }

    @Override
    public long count() {
        return realMap.count();
    }

    @Override
    public void set(long location) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exist(long location) {
        return realMap.exist(location);
    }

    @Override
    public void set(int location) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exist(int location) {
        return realMap.exist(location);
    }

    @Override
    public void clear(int location) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear(long location) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getKeyname() {
        return realMap.getKeyname();
    }

    @Override
    public String getBizname() {
        return realMap.getBizname();
    }

    @Override
    public void serialize(String filename) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bitmap deSerialize(Set<Integer> highbits) {
        return realMap.deSerialize(null);
    }
    
    @Override
    public long lastUpdate() {
        return realMap.lastUpdate();
    }
    
    @Override
    public long getByteSize() {
        return realMap.getByteSize();
    }

    @Override
    public void close() throws IOException {
        realMap.close();
    }

    @Override
    public STATUS getStatus() {
        return realMap.getStatus();
    }

    @Override
    public Set<Integer> highBits() {
      return realMap.highBits();
    }

    @Override
    public long combile(Bitmap map) {
      return realMap.combile(map);
    }
    
    public Bitmap getRealMap() {
      return realMap;
    }

    @Override
    public long fix() {
      return realMap.fix();
    }

    @Override
    public void set(String location) {
      realMap.set(location);
    }

    @Override
    public boolean exist(String location) {
      return realMap.exist(location);
    }

    @Override
    public List<ByteString> serializeToBytes() throws IOException {
      return realMap.serializeToBytes();
    }

    @Override
    public String getStorageName() {
      return realMap.getStorageName();
    }

    @Override
      public boolean isModified() {
        return realMap.isModified();
      }

    @Override
    public long getHitCount() {
      return realMap.getHitCount();
    }

    @Override
    public long setHitCount(long value) {
      return realMap.setHitCount(value);
    }
}
