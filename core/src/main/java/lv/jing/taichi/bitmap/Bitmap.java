package lv.jing.taichi.bitmap;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.protobuf.ByteString;

/**
 * Interface for the bitmap. A bitmap can be implemented as RoaringBitmap or
 * other compressed bitmaps, which can be serialized to disk/hdfs; or use
 * Redis/cache server for fast calculation.
 * 
 * @author jing.lv
 *
 */
public interface Bitmap extends Closeable {
    
    static enum BitMapType {LOCAL, CACHE};
    
    enum STATUS{INMEM, SERIALIZING, DESERIALIZING, INFILE, BOTH, CLOSED}
    
    STATUS getStatus();
    
    BitMapType getType();

	/**
	 * The number of the bits that set, usually for uv count
	 * 
	 * @return the number of the bits
	 */
	long count();

	/**
	 * Usually integer is more than enough, but we offer long-size bitmap
	 * 
	 * @param location the location for the bit to be set
	 */
	void set(long location);

	boolean exist(long location);

	void set(int location);

	boolean exist(int location);
	
  void set(String location);

  boolean exist(String location);
	
	void clear(int location);
	
	void clear(long location);
	
	String getKeyname();
	
	String getBizname();
	
	String getStorageName();

	/**
	 * Serialize to disk
	 * 
	 * @param filename if it's null, use default file name
	 * @throws IOException
	 */
	void serialize(String filename) throws IOException;

	/**
	 * de-serialize and restore the bitmap
	 * 
	 * @param highbits to load all high bit stored file
	 * @return the restored bitmap
	 */
	Bitmap deSerialize(Set<Integer> highbits);
	
	long getByteSize();
	
	long lastUpdate();
	
	/**
	 * consume the given bitmap
	 *  
	 * @param map
	 * @return
	 */
	long combile(Bitmap map);
	
	/**
	 * Offer the information about the bits above Integer.MAX
	 * @return
	 */
	Set<Integer> highBits();
	
	 /**
   * quick scan and fix if necessary
   *  
   * @param map
   * @return
   */
  long fix();
  
  List<ByteString> serializeToBytes() throws IOException;
  
  // if modified after last save
  boolean isModified();
  
  /**
   * 
   * @return total count of all hit
   */
  long getHitCount();
  
  /**
   * set total count of all hit
   */
  long setHitCount(long value);

}
