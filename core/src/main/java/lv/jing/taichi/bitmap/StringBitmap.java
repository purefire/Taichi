package lv.jing.taichi.bitmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import org.roaringbitmap.RoaringBitmap;

import lv.jing.taichi.bitmap.Bitmap.STATUS;

/**
 * String bitmap is NOT a bloom filter implemented on bit array
 * Bloom filter has a posibility of information loss thus it can be <0.1% inaccurate
 * Bloom filter suppport AND/OR which still have a possibility of accuration loss
 * 
 * @author jing.lv
 *
 */
public class StringBitmap extends LocalBitmap {
  
  long count;
  
  int hashSize = 20;

  int[] seeds = { 3, 5, 7, 11, 13, 17, 19, 23, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83 };
  
  private String getLocalFilename() {
    return biz + "_" + date + ".sbtm";
  }

  int[] hashCode(String str) {
    int[] hs = new int[hashSize];
    for (int j = 0; j < hashSize; j++) {
      int h = 0;
      for (int i = 0; i < str.length(); i++)
        h = h * seeds[j] + str.charAt(i);
      hs[j] = h & 0x7FFFFFFF;
    }
    return hs;
  }
  
  public void sethashSize(int size) {
    if (this.seeds.length > size)
      this.hashSize = size;
  }

  public boolean exist(String str) {
    int[] hs = hashCode(str);
    boolean flag = true;
    for (int h : hs)
      flag = flag && super.exist(h);
    return flag;
  }

  public void set(String str) {
    if (!exist(str)) {
      count ++;
      int[] hs = hashCode(str);
      for (int h : hs)
        super.set(h);
      counter.increase();
    }
  }
  
  /*
   * a utility for init with filename, usually as an input of external data
   */
  public StringBitmap(RoaringBitmap rb) {
    super(rb);
    this.count = -1;
  }

  public StringBitmap(String biz, String date, Set<Integer> set) {
    super(biz, date, set);
  }

  /*
   * a utility for init with filename, usually as an input of external data
   */
  public StringBitmap(String biz, String date, String filename) {
    super(biz, date, filename);
  }

  /*
   * note this may be exact count or calculated count (maybe some accuration loss)
   */
  @Override
  public long count() {
    if (this.count != -1)
      return this.count;
    else {
      return calCount();
    }
  }
  
  private long calCount() {
    return -Math
        .round(Math.log(1 - Double.valueOf(super.rb.getCardinality()) / Integer.MAX_VALUE) * Integer.MAX_VALUE / 17);
  }

  @Override
  public synchronized void clear(int location) {
    throw new UnsupportedOperationException();
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
    StringBitmap returnbtm = new StringBitmap(result);
    returnbtm.highrbs = highbits;
    return new ImmutableBitmap(returnbtm);
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
    StringBitmap returnbtm = new StringBitmap(result);
    returnbtm.highrbs = highbits;
    return new ImmutableBitmap(returnbtm);
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
    StringBitmap returnbtm = new StringBitmap(result);
    returnbtm.highrbs = highbits;
    return new ImmutableBitmap(returnbtm);
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
  
  
  public static void main(String[] args) {
    StringBitmap sbm1 = new StringBitmap("test", "1111", (Set)null);
    StringBitmap sbm2 = new StringBitmap("test2", "1111", (Set)null);
    sbm1.sethashSize(16);
    sbm2.sethashSize(16);
    Random rd = new Random();
    int samecount = 0;
    long start = System.currentTimeMillis();
    Set<Integer> set = new HashSet<Integer>();
    for (int i = 0; i < 10000000; i++) {
      int value = rd.nextInt(Integer.MAX_VALUE);
      set.add(value);
      sbm1.set(String.valueOf(value));
    }
    for (int i = 0; i < 12000000; i++) {
      int value = rd.nextInt(Integer.MAX_VALUE);
      if (set.contains(value)) {
        samecount ++;
      }
      sbm2.set(String.valueOf(value));
    }
    System.out.println("time1="+(System.currentTimeMillis() - start));
    System.out.println(sbm1.count());
    System.out.println(sbm2.count());
    System.out.println("time2="+(System.currentTimeMillis() - start));
    sbm1.count = -1;
    sbm2.count = -1;
    System.out.println(sbm1.count());
    System.out.println(sbm2.count());
    System.out.println("same count ="+samecount);
    System.out.println(StringBitmap.AND(new Bitmap[] {sbm1, sbm2}).count());
    System.out.println(StringBitmap.OR(new Bitmap[] {sbm1, sbm2}).count());
    System.out.println("time3="+(System.currentTimeMillis() - start));
  }
}
