package lv.jing.taichi.bitmap;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import lv.jing.taichi.statistics.Counter;

import redis.clients.jedis.BitOP;
import redis.clients.jedis.JedisCluster;

/**
 * The implementation for bitmap which stored in cache middleware like redis or
 * cb. Note: This implementation may not be faster than local one (considering
 * the network cost), but may be more stable/HA using middleware manage system;
 * however due to cache system limitation, it may not hold too many bitmaps.
 */
public class CacheBitmap implements Bitmap {
    
    private STATUS status;
    
    private long lastUpdte;
    
    private Counter counter;

    @Inject
    static JedisCluster jc;

    private String biz;
    private Date date;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    private String getLocalFilename() {
        return biz + "_" + sdf.format(date) + ".btm";
    }

    @Override
    public String getKeyname() {
        return biz + "_" + sdf.format(date);
    }

    public CacheBitmap(String biz, Date date) {
        this.biz = biz;
        this.date = date;
        this.counter = new Counter(biz, sdf.format(date));
        this.status = Bitmap.STATUS.INMEM;
    }

    @Override
    public void set(long location) {
        jc.setbit(getKeyname(), location, true);
    }

    @Override
    public boolean exist(long location) {
        return jc.getbit(getKeyname(), location);
    }

    @Override
    public void set(int location) {
        jc.setbit(getKeyname(), location, true);
    }

    @Override
    public boolean exist(int location) {
        return jc.getbit(getKeyname(), location);
    }

    @Override
    public long count() {
        return jc.bitcount(getKeyname());
    }

    @Override
    public void serialize(String filename) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bitmap deSerialize(Set<Integer> highbits) {
        throw new UnsupportedOperationException();
    }

    static public Bitmap AND(Bitmap b1, Bitmap b2) {
        if (b1.getBizname().equals(b2.getBizname()))
            return null;
        if ((b1.getType() == b2.getType()) && b1.getType() == BitMapType.CACHE) {
            String tempcache = "temp.AND." + b1.getKeyname() + "." + b2.getKeyname();
            jc.bitop(BitOP.AND, tempcache, b1.getKeyname(), b2.getKeyname());
        }
        return new ImmutableBitmap(new CacheBitmap(b1.getBizname(), (Date) null));
    }
    
    @Override
    public STATUS getStatus() {
        return status;
    }

    @Override
    public void clear(int location) {
        jc.setbit(getKeyname(), location, false);
    }

    @Override
    public void clear(long location) {
        jc.setbit(getKeyname(), location, false);
    }

    @Override
    public BitMapType getType() {
        return BitMapType.CACHE;
    }

    @Override
    public String getBizname() {
        return biz;
    }

    @Override
    public long lastUpdate() {
        return lastUpdte;
    }
    
    @Override
    public long getByteSize() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        jc.del(getKeyname());
        this.status = Bitmap.STATUS.CLOSED;
    }
    
    @Override
    public Set<Integer> highBits() {
      return new HashSet<Integer>();
    }

    @Override
    public long combile(Bitmap map) {
      throw new NotImplementedException();
    }

    @Override
    public long fix() {
      return 0;
    }

    @Override
    public void set(String location) {
      throw new NotImplementedException();
    }

    @Override
    public boolean exist(String location) {
      throw new NotImplementedException();
    }
    
    @Override
    public List<ByteString> serializeToBytes() throws IOException {
      throw new NotImplementedException();
    }

    @Override
    public String getStorageName() {
      return getLocalFilename();
    }
    
    @Override
      public boolean isModified() {
        return false;
      }

    @Override
    public long getHitCount() {
      return counter.get();
    }
    
    @Override
    public long setHitCount(long v) {
      return counter.increase(v);
    }
}
