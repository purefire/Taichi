package lv.jing.taichi.bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

import com.google.protobuf.ByteString;
import lv.jing.taichi.statistics.Counter;

/**
 * The implementation for file-mapping bitmap which can be used for UV storage,
 * etc. Note: This refined implement has ability to store data larger than
 * unsigned integer(which is 4,294,967,295), however it's strongly recommended
 * to store data that is no more than 100 * int.MAX
 */
public class LocalBitmap implements Bitmap {

  STATUS status = STATUS.INMEM;
  
  private static final int MAX_BIT = 16;
  
  Counter counter;

  protected long lastUpdte;

  protected RoaringBitmap rb;
  // represent rbs that larger than Int.MAX
  protected HashMap<Integer, RoaringBitmap> highrbs = new HashMap<Integer, RoaringBitmap>();

  protected String biz;
  protected String date;
  
  boolean modified = false;

  int[] seeds = { 3, 5, 7, 11, 13, 17, 19, 23, 31, 37, 41, 43, 47, 53, 59, 61, 67 };

  private String getLocalFilename() {
    return "/data/bitmap/store/"+date+"/"+biz + "_" + date + ".btm";
  }

  /*
   * only used for bit map operations, no need to do serialization
   * 
   * @param rb that already calculated
   */
  protected LocalBitmap(RoaringBitmap rb) {
    this.rb = rb;
    this.lastUpdte = System.currentTimeMillis();
  }

  /**
   * The constructor follow the logic: 1. if no record, create brand new one that
   * in memory 2. if recorded, do de-serialize from file and recover
   * 
   * @param biz
   * @param date
   */
  public LocalBitmap(String biz, String date, Set<Integer> set) {
    this.biz = biz;
    this.date = date;
    this.counter = new Counter(biz, date);
    rb = new RoaringBitmap();
    if (set != null) {
      deSerialize(set);
      status = STATUS.BOTH;
    } else {
      status = STATUS.INMEM;
    }
  }

  /*
   * a utility for init with filename, usually as an input of external data
   */
  public LocalBitmap(String biz, String date, String filename) {
    this.biz = biz;
    this.date = date;
    this.counter = new Counter(biz, date);
    rb = new RoaringBitmap();
    deSerialize(filename);
    status = STATUS.INMEM;
  }

  @Override
  public synchronized void set(long location) {
    if (location > ((long)Integer.MAX_VALUE) * MAX_BIT) {
      counter.increase();
      lastUpdte = System.currentTimeMillis();
      modified = true;
      return;
    }
    if (location > Integer.MAX_VALUE) {
      long wit = location / Integer.MAX_VALUE;
      // wit is no more than int.MAX
      RoaringBitmap hrb = highrbs.get((int) wit);
      if (hrb == null) {
        hrb = new RoaringBitmap();
      }
      hrb.add((int) (location - Integer.MAX_VALUE * wit));
      highrbs.put((int) wit, hrb);
    } else {
      rb.add((int) location);
    }
    counter.increase();
    lastUpdte = System.currentTimeMillis();
    modified = true;
  }

  @Override
  public boolean exist(long location) {
    if (location > Integer.MAX_VALUE) {
      long wit = location / Integer.MAX_VALUE;
      // wit is no more than int.MAX
      RoaringBitmap hrb = highrbs.get((int) wit);
      if (hrb == null) {
        return false;
      }
      return hrb.contains((int) (location - Integer.MAX_VALUE * wit));
    }
    return rb.contains((int) location);
  }
  
  public boolean isModified() {
    return modified;
  }

  int[] hashCode(String str) {
    int[] hs = new int[seeds.length];
    for (int j = 0; j < seeds.length; j++) {
      int h = 0;
      for (int i = 0; i < str.length(); i++)
        h = h * seeds[j] + str.charAt(i);
      hs[j] = h & 0x7FFFFFFF;
    }
    return hs;
  }

  public boolean exist(String str) {
    int[] hs = hashCode(str);
    boolean flag = true;
    for (int h : hs)
      flag = flag && rb.contains(h);
    return flag;
  }

  public void set(String str) {
    int[] hs = hashCode(str);
    for (int h : hs)
      rb.add(h);
    counter.increase();
    lastUpdte = System.currentTimeMillis();
    modified = true;
  }

  @Override
  public synchronized void set(int location) {
    rb.add(location);
    counter.increase();
    modified = true;
    lastUpdte = System.currentTimeMillis();
  }

  @Override
  public boolean exist(int location) {
    return rb.contains(location);
  }

  @Override
  public long count() {
    Long[] count = new Long[1];
    count[0] = 0L;
    highrbs.forEach((k, v) -> {
      count[0] += v.getCardinality();
    });
    return count[0] + rb.getCardinality();
  }

  /**
   * Save to file and record in db
   */
  @Override
  public synchronized void serialize(String filename) throws IOException {
    if (status == STATUS.CLOSED)
      return;
    String outfile = filename;
    if (outfile == null || outfile.length() == 0) {
      outfile = getLocalFilename();
    }

    try (DataOutputStream out = new DataOutputStream(new FileOutputStream(outfile))) {
      rb.serialize(out);
    }

    Set<Entry<Integer, RoaringBitmap>> entries = highrbs.entrySet();
    for (Entry<Integer, RoaringBitmap> entry : entries) {
      try (DataOutputStream hout = new DataOutputStream(new FileOutputStream(outfile + "." + entry.getKey()))) {
        entry.getValue().serialize(hout);
      }
    }
    modified = false;
    status = STATUS.BOTH;
  }
  
  /**
   * Save to file and record in db
   */
  public synchronized List<ByteString> serializeToBytes() throws IOException {
    if (status == STATUS.CLOSED)
      return null;

    List<ByteString> bitmapData = new ArrayList<ByteString>();
    
    ByteArrayOutputStream outdata = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(outdata)) {
      rb.serialize(out);
    }
    
    ByteString bs = ByteString.copyFrom(outdata.toByteArray());
    bitmapData.add(0, bs);

    Set<Entry<Integer, RoaringBitmap>> entries = highrbs.entrySet();
    for (Entry<Integer, RoaringBitmap> entry : entries) {
      ByteArrayOutputStream temp = new ByteArrayOutputStream();
      try (DataOutputStream hout = new DataOutputStream(temp)) {
        entry.getValue().serialize(hout);
      }
      ByteString tmp = ByteString.copyFrom(temp.toByteArray());
      bitmapData.add(entry.getKey(), tmp);
    }
    status = STATUS.BOTH;
    return bitmapData;
  }

  // there is a bug of serial/de-serial of RBM, a workaround here before the
  // community fix it
  public synchronized long fix() {
    RoaringBitmap replace = new RoaringBitmap();
    rb.forEach(new IntConsumer() {
      @Override
      public void accept(int value) {
        replace.add(value);
      }
    });
    this.rb = replace;
    final HashMap<Integer, RoaringBitmap> remap = new HashMap<Integer, RoaringBitmap>();
    highrbs.entrySet().forEach(new Consumer<Entry<Integer, RoaringBitmap>>() {
      @Override
      public void accept(Entry<Integer, RoaringBitmap> arg0) {
        RoaringBitmap rehmap = new RoaringBitmap();
        arg0.getValue().forEach(new IntConsumer() {
          @Override
          public void accept(int value) {
            rehmap.add(value);
          }
        });
        remap.put(arg0.getKey(), rehmap);
      }
    });
    this.highrbs = remap;
    return this.count();
  }

  public synchronized void saveToIntFile(String filename) throws FileNotFoundException, IOException {
    if (filename == null || filename.length() == 0) {
      return;
    } else {
      try (DataOutputStream out = new DataOutputStream(new FileOutputStream(filename))) {
        rb.forEach(new IntConsumer() {
          @Override
          public void accept(int value) {
            try {
              out.write(String.valueOf(value).getBytes());
              out.writeChar('\n');
            } catch (IOException e) {
              System.err.println(e);
              e.printStackTrace();
            }
          }
        });
        highrbs.entrySet().forEach(new Consumer<Map.Entry<Integer, RoaringBitmap>>() {

          @Override
          public void accept(Map.Entry<Integer, RoaringBitmap> rbe) {
            long key = rbe.getKey();
            rbe.getValue().forEach(new IntConsumer() {
              @Override
              public void accept(int value) {
                try {
                  out.write(String.valueOf(value + key * Integer.MAX_VALUE).getBytes());
                  out.writeChar('\n');
                } catch (IOException e) {
                  System.err.println(e);
                  e.printStackTrace();
                }
              }
            });
          }
        });
      } catch (Exception e) {
        System.err.println(e);
        e.printStackTrace();
      }
    }
  }

  @Override
  public synchronized Bitmap deSerialize(Set<Integer> highbits) {
    if (status == STATUS.CLOSED)
      return null;
    status = STATUS.DESERIALIZING;
    String filename = getLocalFilename();
    try (DataInputStream in = new DataInputStream(new FileInputStream(filename))) {
      rb.deserialize(in);
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println(this.getBizname()+this.getKeyname()+" Load failed!");
      rb = new RoaringBitmap();
    }
    if (highbits != null)
      highbits.forEach(bit -> {
        try (DataInputStream in = new DataInputStream(new FileInputStream(filename + "." + bit))) {
          RoaringBitmap trb = new RoaringBitmap();
          trb.deserialize(in);
          highrbs.put(bit, trb);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    status = STATUS.BOTH;
    this.lastUpdte = System.currentTimeMillis();
    return this;
  }

  private synchronized Bitmap deSerialize(String filename) {
    if (status == STATUS.CLOSED)
      return null;
    status = STATUS.DESERIALIZING;
    try (DataInputStream in = new DataInputStream(new FileInputStream(filename))) {
      rb.deserialize(in);
    } catch (IOException e) {
      e.printStackTrace();
    }
    this.lastUpdte = System.currentTimeMillis();
    status = STATUS.BOTH;
    return this;
  }

  static public Bitmap AND(Bitmap b1, Bitmap b2) {
    if ((b1.getType() == b2.getType()) && b1.getType() == BitMapType.LOCAL) {
      RoaringBitmap rb = RoaringBitmap.and(((LocalBitmap) b1).rb, ((LocalBitmap) b2).rb);
      LocalBitmap ret = new LocalBitmap(rb);
      Set<Integer> set = new HashSet<Integer>();
      LocalBitmap lbm1 = ((LocalBitmap) b1);
      LocalBitmap lbm2 = ((LocalBitmap) b2);
      set.addAll(lbm1.highBits());
      set.addAll(lbm2.highBits());
      HashMap<Integer, RoaringBitmap> highbits = new HashMap<Integer, RoaringBitmap>();
      set.forEach(bit -> {
        if (lbm1.highrbs.get(bit) != null) {
          if ((lbm2.highrbs.get(bit) != null)) {
            highbits.put(bit, RoaringBitmap.and(lbm1.highrbs.get(bit), lbm2.highrbs.get(bit)));
          }
        }
      });
      ret.highrbs = highbits;
      return new ImmutableBitmap(ret);
    }
    return null;
  }
  
  static public Bitmap AND(Bitmap[] btms) {
    Set<Integer> set = new HashSet<Integer>();
    if (btms[0] == null) return EmptyLocalBitmap.getInstance();
    RoaringBitmap result = ((LocalBitmap)btms[0]).rb.clone();
    set.addAll(((LocalBitmap)btms[0]).highBits());
    final HashMap<Integer, RoaringBitmap> highbits = new HashMap<Integer, RoaringBitmap>();
    set.forEach(bit -> {
      highbits.put(bit, ((LocalBitmap)btms[0]).highrbs.get(bit));
    });
    for (int i = 1; i < btms.length; i++) {
      final LocalBitmap tmp = (LocalBitmap)btms[i];
      if (tmp == null) return EmptyLocalBitmap.getInstance();
      result = RoaringBitmap.and(tmp.rb, result);
      set.addAll(((LocalBitmap)btms[i]).highBits());
      set.forEach(bit -> {
        if (highbits.get(bit) != null && tmp.highrbs.get(bit) != null) {
            highbits.put(bit, RoaringBitmap.and(tmp.highrbs.get(bit), highbits.get(bit)));
          }
        });
    }
    LocalBitmap returnbtm = new LocalBitmap(result);
    returnbtm.highrbs = highbits;
    return new ImmutableBitmap(returnbtm);
  }

  static public Bitmap OR(Bitmap b1, Bitmap b2) {
    if ((b1.getType() == b2.getType()) && b1.getType() == BitMapType.LOCAL) {
      RoaringBitmap rb = RoaringBitmap.or(((LocalBitmap) b1).rb, ((LocalBitmap) b2).rb);
      LocalBitmap ret = new LocalBitmap(rb);
      Set<Integer> set = new HashSet<Integer>();
      LocalBitmap lbm1 = ((LocalBitmap) b1);
      LocalBitmap lbm2 = ((LocalBitmap) b2);
      set.addAll(lbm1.highBits());
      set.addAll(lbm2.highBits());
      HashMap<Integer, RoaringBitmap> highbits = new HashMap<Integer, RoaringBitmap>();
      set.forEach(bit -> {
        if (lbm1.highrbs.get(bit) != null) {
          if ((lbm2.highrbs.get(bit) != null)) {
            highbits.put(bit, RoaringBitmap.or(lbm1.highrbs.get(bit), lbm2.highrbs.get(bit)));
          } else {
            highbits.put(bit, lbm1.highrbs.get(bit).clone());
          }
        } else if ((lbm2.highrbs.get(bit) != null)) {
          highbits.put(bit, lbm2.highrbs.get(bit).clone());
        }
      });
      ret.highrbs = highbits;
      return new ImmutableBitmap(ret);
    }
    return null;
  }
  
  static public Bitmap OR(Bitmap[] btms) {
    if (btms.length == 0) return EmptyLocalBitmap.getInstance();
    Set<Integer> set = new HashSet<Integer>();
    RoaringBitmap result = ((LocalBitmap) btms[0]).rb.clone();
    set.addAll(((LocalBitmap) btms[0]).highBits());
    final HashMap<Integer, RoaringBitmap> highbits = new HashMap<Integer, RoaringBitmap>();
    set.forEach(bit -> {
      highbits.put(bit, ((LocalBitmap)btms[0]).highrbs.get(bit));
    });
    for (int i = 1; i < btms.length; i++) {
      final LocalBitmap tmp = (LocalBitmap) btms[i];
      result = RoaringBitmap.or(tmp.rb, result);
      set.addAll(((LocalBitmap) btms[i]).highBits());
      set.forEach(bit -> {
        if (highbits.get(bit) != null) {
          if (tmp.highrbs.get(bit) != null) {
            highbits.put(bit, RoaringBitmap.or(tmp.highrbs.get(bit), highbits.get(bit)));
          }
        } else if (tmp.highrbs.get(bit) != null) {
          highbits.put(bit, tmp.highrbs.get(bit).clone());
        }
      });
    }
    LocalBitmap returnbtm = new LocalBitmap(result);
    returnbtm.highrbs = highbits;
    return new ImmutableBitmap(returnbtm);
  }

  static public Bitmap XOR(Bitmap b1, Bitmap b2) {
    if ((b1.getType() == b2.getType()) && b1.getType() == BitMapType.LOCAL) {
      RoaringBitmap rb = RoaringBitmap.xor(((LocalBitmap) b1).rb, ((LocalBitmap) b2).rb);
      LocalBitmap ret = new LocalBitmap(rb);
      Set<Integer> set = new HashSet<Integer>();
      LocalBitmap lbm1 = ((LocalBitmap) b1);
      LocalBitmap lbm2 = ((LocalBitmap) b2);
      set.addAll(lbm1.highBits());
      set.addAll(lbm2.highBits());
      HashMap<Integer, RoaringBitmap> highbits = new HashMap<Integer, RoaringBitmap>();
      set.forEach(bit -> {
        if (lbm1.highrbs.get(bit) != null) {
          if ((lbm2.highrbs.get(bit) != null)) {
            highbits.put(bit, RoaringBitmap.xor(lbm1.highrbs.get(bit), lbm2.highrbs.get(bit)));
          } else {
            highbits.put(bit, lbm1.highrbs.get(bit).clone());
          }
        } else if ((lbm2.highrbs.get(bit) != null)) {
          highbits.put(bit, lbm2.highrbs.get(bit).clone());
        }
      });
      ret.highrbs = highbits;
      return new ImmutableBitmap(ret);
    }
    return null;
  }
  

  static public Bitmap XOR(Bitmap[] btms) {
    Set<Integer> set = new HashSet<Integer>();
    RoaringBitmap result = ((LocalBitmap)btms[0]).rb.clone();
    set.addAll(((LocalBitmap)btms[0]).highBits());
    final HashMap<Integer, RoaringBitmap> highbits = new HashMap<Integer, RoaringBitmap>();
    set.forEach(bit -> {
      highbits.put(bit, ((LocalBitmap)btms[0]).highrbs.get(bit));
    });
    for (int i = 1; i < btms.length; i++) {
      final LocalBitmap tmp = (LocalBitmap)btms[i];
      result = RoaringBitmap.xor(tmp.rb, result);
      set.addAll(((LocalBitmap)btms[i]).highBits());
      set.forEach(bit -> {
        if (highbits.get(bit) != null) {
          if (tmp.highrbs.get(bit) != null) {
            highbits.put(bit, RoaringBitmap.xor(tmp.highrbs.get(bit), highbits.get(bit)));
          }
        } else if (tmp.highrbs.get(bit) != null) {
          highbits.put(bit, tmp.highrbs.get(bit).clone());
        }
      });
    }
    LocalBitmap returnbtm = new LocalBitmap(result);
    returnbtm.highrbs = highbits;
    return new ImmutableBitmap(returnbtm);
  }
  
  static public Bitmap ANDNOT(Bitmap[] btms) {
    Set<Integer> set = new HashSet<Integer>();
    RoaringBitmap result = ((LocalBitmap)btms[0]).rb.clone();
    set.addAll(((LocalBitmap)btms[0]).highBits());
    final HashMap<Integer, RoaringBitmap> highbits = new HashMap<Integer, RoaringBitmap>();
    set.forEach(bit -> {
      highbits.put(bit, ((LocalBitmap)btms[0]).highrbs.get(bit));
    });
    for (int i = 1; i < btms.length; i++) {
      final LocalBitmap tmp = (LocalBitmap)btms[i];
      result = RoaringBitmap.andNot(result,tmp.rb);
      set.addAll(((LocalBitmap)btms[i]).highBits());
      set.forEach(bit -> {
        if (highbits.get(bit) != null) {
          if (tmp.highrbs.get(bit) != null) {
            highbits.put(bit, RoaringBitmap.andNot(highbits.get(bit),tmp.highrbs.get(bit)));
          }
        } else if (tmp.highrbs.get(bit) != null) {
          highbits.put(bit, tmp.highrbs.get(bit).clone());
        }
      });
    }
    LocalBitmap returnbtm = new LocalBitmap(result);
    returnbtm.highrbs = highbits;
    return new ImmutableBitmap(returnbtm);
  }

  @Override
  public synchronized void clear(int location) {
    modified = true;
    lastUpdte = System.currentTimeMillis();
    rb.remove(location);
  }

  @Override
  public synchronized void clear(long location) {
    if (location > Integer.MAX_VALUE) {
      long wit = location / Integer.MAX_VALUE;
      // wit is no more than int.MAX
      RoaringBitmap hrb = highrbs.get((int) wit);
      if (hrb == null) {
        return;
      }
      hrb.remove((int) (location - Integer.MAX_VALUE * wit));
    } else
      rb.remove((int) location);
    modified = true;
    lastUpdte = System.currentTimeMillis();
  }

  @Override
  public BitMapType getType() {
    return BitMapType.LOCAL;
  }

  @Override
  public String getKeyname() {
    return date;
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
    Long[] count = new Long[1];
    count[0] = 0L;
    highrbs.forEach((k, v) -> {
      count[0] += v.getSizeInBytes();
    });
    return count[0] + rb.getSizeInBytes();
  }

  @Override
  public void close() throws IOException {
    if (status == STATUS.CLOSED)
      return;
    this.status = Bitmap.STATUS.CLOSED;
    highrbs.forEach((k, v) -> {
      v.clear();
    });
    rb.clear();
  }

  @Override
  public STATUS getStatus() {
    return status;
  }

  @Override
  public Set<Integer> highBits() {
    Set<Integer> highSet = new HashSet<Integer>();
    highrbs.forEach((k, v) -> {
      highSet.add(k);
    });
    return highSet;
  }

  @Override
  public synchronized long combile(Bitmap map) {
    if (map instanceof LocalBitmap) {
      rb.or(((LocalBitmap) map).rb);
      modified = true;
      lastUpdte = System.currentTimeMillis();
    }
    return this.count();
  }

  @Override
  public String getStorageName() {
    return getLocalFilename();
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
