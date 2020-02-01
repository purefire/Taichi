package lv.jing.taichi.bitmap;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.and;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.lang3.time.DateUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.protobuf.ByteString;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.lexer.token.OperatorType;
import lv.jing.taichi.bitmap.Bitmap.STATUS;
import lv.jing.taichi.core.conf.TaichiConf;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;

/**
 * The factory of bit maps Use the factory to create, utilize, store/load the
 * bit maps, and/or compare the bitmaps for further calculation.
 *
 * Note: difference type of bitmap may not be operated in a proper way Note: All
 * date should transformed to to "yyyyMMdd" format
 *
 * @author jing.lv
 *
 */
@Singleton
public class BitMapManager {

  private static final String GROUP_COLLECTION_NAME = "GroupList";
  private static final String BITMAP_COLLECTION_NAME = "BitmapList";
  // just placeholder for shutdown future, difference to other bit-maps
  private static final String DEFAULT_COLUMN = "11111111";
  private static final String SHUTDOWN_ROW = "SHUTDOWN";
  private static final String MAINTAIN_ROW = "MAINTAIN";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  Set<Bitmap> toRemove = new HashSet<>();

  @Inject
  private MongoDatabase mongoDatabase;

  @Inject
  private TaichiConf conf;

  final ExecutorService exec = Executors.newCachedThreadPool();

  ExecutorService maintainanceExec = Executors.newCachedThreadPool();

  // Max memory size, default 1G
  static long MAX_MEM_SIZE = 1000000000;

  static Table<String, String, Bitmap> bitmapTable;
  static Table<String, String, Bitmap> StringBitmapTable;

  static Table<String, String, Future<Long>> futureTable;
  static Table<String, String, Future<Bitmap>> rawFutureTable;
  static Table<String, String, Future<HashMap<Integer, Double>>> mapFutureTable;

  private static final String COMMON_DATE = "yyyyMMdd";

  private HashMap<String, BitmapGroup> bmgroupMap;

  static public String formatDateToStr(Date date) {
    SimpleDateFormat sdf = new SimpleDateFormat(COMMON_DATE);
    return sdf.format(date);
  }

  static public Date parseStrToDate(String date) throws ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat(COMMON_DATE);
    return sdf.parse(date);
  }

  public void loadAll(String startDate) {
    MongoCollection<Document> collect = this.mongoDatabase.getCollection(BITMAP_COLLECTION_NAME);
    if (null == collect) {
      return;
    }
    int[] c = new int[2];

    FindIterable<Document> fi = collect.find(eq("name", startDate));
    fi.forEach((Block<? super Document>) doc -> {
      String date = null;
      String highbits = null;
      String bizname = null;
      long hitcount=0;
      boolean isStringbm = false;
      for (Entry<String, Object> entry : doc.entrySet()) {
        if (entry.getKey().equals("bizname"))
          bizname = (String) entry.getValue();
        if (entry.getKey().equals("name"))
          date = (String) entry.getValue();
        if (entry.getKey().equals("highbits"))
          highbits = (String) entry.getValue();
        if (entry.getKey().equals("type") && entry.getValue().equals("S")) 
          isStringbm = true;
        if (entry.getKey().equals("count"))
          hitcount = (long) entry.getValue();
      }
      if (date != null) {
        try {
          if (Integer.valueOf(date) >= Integer.valueOf(startDate)) {
            log.error("One load [" + bizname + ":" + date + "], > " + startDate);
            Set<Integer> set = new HashSet<Integer>();

            if (highbits != null && highbits.length() != 0) {
              String[] bits = highbits.split(",");
              for (String bit : bits) {
                set.add(Integer.valueOf(bit));
              }
            }
            LocalBitmap lbm = null;
            try {
              lbm = isStringbm? new StringBitmap(bizname, date, set) : new LocalBitmap(bizname, date, set);
            } catch (Exception e) {
              try {
                String filename = getLocalFilename(bizname, date);
                remoteLoad(filename, getossname(bizname,date));
                for (int bit : set) {
                  remoteLoad(filename + "." + bit, getossname(bizname,date) + "." + bit);
                }
              } catch (IOException e1) {
                e1.printStackTrace();
              }
              lbm = isStringbm? new StringBitmap(bizname, date, set) : new LocalBitmap(bizname, date, set);
            }
            if (lbm != null) {
              lbm.setHitCount(hitcount);
              bitmapTable.put(bizname, date, lbm);
              c[0]++;
            } else {
              c[1]++;
            }
            
          }
        } catch (java.lang.NumberFormatException e) {
          // ignore
        }
      }
    });

    log.debug(c[0] + " items loaded");
    log.debug(c[1] + " items loading failed");
  }

  @Inject
  public BitMapManager() {
    bitmapTable = HashBasedTable.create();
    StringBitmapTable = HashBasedTable.create();
    futureTable = HashBasedTable.create();
    rawFutureTable = HashBasedTable.create();
    mapFutureTable = HashBasedTable.create();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      bitmapTable.cellSet().forEach(entry -> {
        Bitmap bm = entry.getValue();
        try {
          save(bm);
        } catch (IOException e) {
          // do nothing
        }
      });
      saveGroup();
    }));

    // TODO enable when neccessary
    // hdfsHelper = new HDFSHelper();
  }

  public void init() {

    loadAll(formatDateToStr(new Date()));

    this.bmgroupMap = loadGroup();

    AviatorEvaluator.addFunction(new AndFunction(this));
    AviatorEvaluator.addFunction(new OrFunction(this));
    AviatorEvaluator.addFunction(new CountFunction(this));
    AviatorEvaluator.addFunction(new debugFunction(this));
    AviatorEvaluator.addFunction(new AndNotFunction(this));
    AviatorEvaluator.addOpFunction(OperatorType.AND, new AndOperation(this));
    AviatorEvaluator.addOpFunction(OperatorType.OR, new OrOperation(this));
    AviatorEvaluator.addOpFunction(OperatorType.DIV, new DivideOperation(this));
  }

  private void remoteSave(Bitmap bm) {
    // TODO this is desinged to do persistence to some remote storage system, like HDFS or object storage system
  }

  private void remoteLoad(Bitmap bm) throws IOException {
    remoteLoad(getLocalFilename(bm.getBizname(), bm.getKeyname()), bm.getStorageName());
    return;
  }

  private void remoteLoad(String filename, String ossname) throws IOException {
    // TODO load from some remote storage system
  }

  private void save(Bitmap bm) throws IOException {
    if (bm.getStatus() == STATUS.CLOSED)
      return;
    log.debug("save to bm:" + bm.getBizname() + bm.getKeyname());
    String bizname = bm.getBizname();
    String date = bm.getKeyname();

    UpdateOptions uop = new UpdateOptions();
    uop.upsert(true);
    Document dc = new Document("name", date);
    dc.append("time", String.valueOf(System.currentTimeMillis()));

    StringBuilder Strhighbits = new StringBuilder();
    Set<Integer> intSet = bm.highBits();
    for (Integer integer : intSet) {
      Strhighbits.append(String.valueOf(integer)).append(",");
    }

    BasicDBObject updateDocument = new BasicDBObject();
    updateDocument.append("time", String.valueOf(System.currentTimeMillis()));
    updateDocument.append("highbits", Strhighbits.toString());
    updateDocument.append("count", bm.getHitCount());
    updateDocument.append("bizname", bm.getBizname());
    if (bm instanceof StringBitmap) {
      updateDocument.append("type", "S");
    }
    MongoCollection<Document> collect = mongoDatabase.getCollection(BITMAP_COLLECTION_NAME);
    if (null == collect) {
      mongoDatabase.createCollection(BITMAP_COLLECTION_NAME);
      collect = mongoDatabase.getCollection(BITMAP_COLLECTION_NAME);
    }
    log.error("save to collection:" + collect.toString());

    BasicDBObject searchQuery = new BasicDBObject().append("name", date).append("bizname", bm.getBizname());
    collect.updateOne(searchQuery,new Document("$set", updateDocument), uop);
    synchronized (bm) {

      if(!new File(getLocalFilename(bm.getBizname(), bm.getKeyname())).getParentFile().exists()) {
          if(!new File(getLocalFilename(bm.getBizname(), bm.getKeyname())).getParentFile().mkdirs()) {
            log.error("Create dir failed");
          }
      }
      bm.serialize(getLocalFilename(bm.getBizname(), bm.getKeyname()));
    }
    remoteSave(bm);
  }

  private void saveGroup() {
    // BasicDBObject updateDocument = new BasicDBObject(this.bmgroupMap);
    if (null == mongoDatabase.getCollection(GROUP_COLLECTION_NAME)) {
      mongoDatabase.createCollection(GROUP_COLLECTION_NAME);
    }
    final MongoCollection<Document> mgc = mongoDatabase.getCollection(GROUP_COLLECTION_NAME);
    UpdateOptions uop = new UpdateOptions();

    uop.upsert(true);
    if (bmgroupMap == null) return;
    bmgroupMap.entrySet().forEach(action -> {
      if (action.getKey() == null || action.getKey().length() == 0)
        return;
      BasicDBObject updateDocument = new BasicDBObject();
      BasicDBList list = new BasicDBList();
      list.addAll(action.getValue().getNameList());
      updateDocument.append("name", action.getKey());
      updateDocument.append("biz", list);
      mgc.updateOne(Filters.eq("name", action.getKey()), new Document("$set", updateDocument), uop);
    });
  }

  // GROUP_COLLECTION_NAME -> {['groupname':xxx, 'biz':[z1,z2...]], ... }
  private HashMap<String, BitmapGroup> loadGroup() {
    this.bmgroupMap = new HashMap<String, BitmapGroup>();
    MongoCollection<Document> collect = this.mongoDatabase.getCollection(GROUP_COLLECTION_NAME);
    FindIterable<Document> fi = collect.find();
    fi.forEach((Block<? super Document>) doc -> {
      String groupname = null;
      String[] bizs = null;
      BitmapGroup bg = new BitmapGroup(null);
      for (Entry<String, Object> entry : doc.entrySet()) {
        if (entry.getKey().equals("name"))
          groupname = (String) entry.getValue();
        bg.name = groupname;
        if (entry.getKey().equals("biz")) {
          ArrayList<String> res = ((ArrayList<String>) entry.getValue());
          if (res != null) {
            res.forEach(entity -> {
              bg.add(entity);
            });
          }
        }
      }
      System.err.println("group name = " + groupname + ",size = " + bg.getSize());
      this.bmgroupMap.put(groupname, bg);
    });
    return this.bmgroupMap;
  }

  private Set<Integer> load(String bizname, String date) {
    MongoCollection<Document> collect = mongoDatabase.getCollection(BITMAP_COLLECTION_NAME);
    log.debug("loading " + bizname);
    if (null == collect) {
      return null;
    }

    BasicDBObject query = new BasicDBObject();
    query.put("name", date);
    Document doc = collect.find(and(eq("name", date),eq("bizname", bizname))).first();
    if (doc != null) {
      Set<Integer> ret = new HashSet<Integer>();
      if (doc.getString("highbits") != null && doc.getString("highbits").length() > 0) {
        String[] highbits = doc.getString("highbits").split(",");
        for (String bit : highbits) {
          ret.add(Integer.valueOf(bit));
        }
      }
      try {
        remoteLoad(getLocalFilename(bizname, date), getossname(bizname, date));
      } catch (IOException e) {
        e.printStackTrace();
      }
      ret.forEach(highbit -> {
        File temp = new File(getLocalFilename(bizname, date));
        if (!temp.exists()) {
          try {
            remoteLoad(getLocalFilename(bizname, date) + "." + highbit, getossname(bizname, date) + "." + highbit);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
      return ret;
    } else {
      return null;
    }
  }
  
  private Bitmap loadBitmap(String bizname, String date) {
    MongoCollection<Document> collect = mongoDatabase.getCollection(BITMAP_COLLECTION_NAME);
    log.debug("loading " + bizname);
    if (null == collect) {
      return null;
    }

    BasicDBObject query = new BasicDBObject();
    query.put("name", date);
    Document doc = collect.find(and(eq("name", date),eq("bizname", bizname))).first();
    if (doc != null) {
      Set<Integer> ret = new HashSet<Integer>();
      if (doc.getString("highbits") != null && doc.getString("highbits").length() > 0) {
        String[] highbits = doc.getString("highbits").split(",");
        for (String bit : highbits) {
          ret.add(Integer.valueOf(bit));
        }
      }
      try {
        remoteLoad(getLocalFilename(bizname, date), getossname(bizname, date));
      } catch (IOException e) {
        e.printStackTrace();
      }
      ret.forEach(highbit -> {
        try {
          remoteLoad(getLocalFilename(bizname, date) + "." + highbit, getossname(bizname, date) + "." + highbit);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });

      Bitmap bm = null;

      if (doc.containsKey("type") && doc.getString("type").equals("S")) {
        bm = new StringBitmap(bizname, date, ret);
      } else {
        bm = new LocalBitmap(bizname, date, ret);
      }
      if (doc.containsKey("count")) {
        bm.setHitCount(doc.getLong("count"));
      }
      bitmapTable.put(bizname, date, bm);
      return bm;
    } else  {
      // try if still exist
      File file = new File(getLocalFilename(bizname, date));
      if (file.exists()) {
        Bitmap bm = new LocalBitmap(bizname, date, new HashSet<Integer>());
        if (bm.count() != 0) {
          bitmapTable.put(bizname, date, bm);
          return bm;
        }
      }
      return null;
    }
  }

  public long combine(String biz, String date, String filename) {
    Bitmap bm = bitmapTable.get(biz, date);
    if (bm == null) {
      bm = loadBitmap(biz, date);
      log.debug("creating bitmap, file load = ", String.valueOf(bm != null));
    }
    Bitmap bmtoc = new LocalBitmap(biz, date, filename);

    return bm.combile(bmtoc);
  }

  public Bitmap create(String biz, Date date, boolean needLong) {
    return create(biz, formatDateToStr(date));
  }

//  public Bitmap create(String biz, String date) {
//    Bitmap bm = bitmapTable.get(biz, date);
//    if (bm == null) {
//      Set<Integer> set = load(biz, date);
//      log.debug("creating bitmap, file load = ", String.valueOf(set != null));
//      bm = new LocalBitmap(biz, date, set);
//      bitmapTable.put(biz, date, bm);
//    }
//    return bm;
//  }
  
  public Bitmap create(String biz, String date) {
    Bitmap bm = bitmapTable.get(biz, date);
    if (bm == null) {
      bm = loadBitmap(biz, date);
    }
    if (bm == null) {
      bm = new LocalBitmap(biz, date, (Set)null);
      bitmapTable.put(biz, date, bm);
    }
    return bm;
  }
  
  private Bitmap createStrbm(String biz, String date) {
    Bitmap bm = StringBitmapTable.get(biz, date);
    if (bm == null) {
      bm = loadBitmap(biz, date);
    }
    if (bm == null) {
      bm = new StringBitmap(biz, date, (Set)null);
      StringBitmapTable.put(biz, date, bm);
    }
    return bm;
  }

  public Bitmap set(String biz, Date date, long bit) {
    return set(biz, formatDateToStr(date), bit);
  }

  public Bitmap set(String biz, String date, long bit) {
    Bitmap bm = bitmapTable.get(biz, date);
    if (bm == null) {
      bm = create(biz, date);
    }
    bm.set(bit);
    return bm;
  }

  public Bitmap set(String biz, String date, String bit) {
    Bitmap bm = bitmapTable.get(biz, date);
    if (bm == null) {
      bm = create(biz, date);
    }
    bm.set(bit);
    return bm;
  }
  
  public Bitmap strSet(String biz, String date, String bit) {
    Bitmap bm = StringBitmapTable.get(biz, date);
    if (bm == null) {
      bm = createStrbm(biz, date);
    }
    bm.set(bit);
    return bm;
  }

  public long fix(String biz, String date) {
    Bitmap bm = bitmapTable.get(biz, date);
    if (bm == null) {
      return -1;
    }
    return bm.fix();
  }

  public boolean get(String biz, Date date, long bit) {
    return get(biz, formatDateToStr(date), bit);
  }

  public boolean get(String biz, String date, long bit) {
    Bitmap bm = bitmapTable.get(biz, date);
    if (bm == null) {
      bm = loadBitmap(biz, date);
    }
    if (bm == null) {
      return false;
    }
    return bm.exist(bit);
  }

  public List<ByteString> offer(String biz, String date) throws IOException {
    Bitmap bm = bitmapTable.get(biz, date);
    if (bm == null) {
      return null;
    }

    return bm.serializeToBytes();
  }

  // debug mode, save all ids to file
  public boolean debugInfile(String biz, String date, String filename) {
    Bitmap bm = bitmapTable.get(biz, date);
    if (bm == null) {
      return false;
    }
    try {
      ((LocalBitmap) bm).saveToIntFile(filename);
    } catch (IOException e) {
      // ignore
    }
    return true;
  }

  // debug mode, save all ids to file
  public boolean debugInfile(String biz, String date, String biz2, String date2, OPERATOR op, String filename) {
    Bitmap bm = bitmapTable.get(biz, date);
    if (bm == null) {
      return false;
    }
    Bitmap bm2 = bitmapTable.get(biz2, date2);
    if (bm2 == null) {
      return false;
    }
    Bitmap map = null;
    switch (op) {
    case and:
      map = LocalBitmap.AND(bm, bm2);
      break;
    case or:
      map = LocalBitmap.OR(bm, bm2);
      break;
    case xor:
      map = LocalBitmap.XOR(bm, bm2);
      break;
    }
    try {
      // TODO avaid casting
      ((LocalBitmap) (((ImmutableBitmap) map).getRealMap())).saveToIntFile(filename);
    } catch (IOException e) {
      // ignore
    }
    return true;
  }

  // debug mode, save all ids to file
  public boolean debugInfile(Bitmap bm, String filename) {
    LocalBitmap debugmap;
    if (bm instanceof ImmutableBitmap) {
      debugmap = (LocalBitmap) ((ImmutableBitmap) bm).getRealMap();
    } else {
      debugmap = (LocalBitmap) bm;
    }
    try {
      debugmap.saveToIntFile(filename);
    } catch (IOException e) {
      // ignore
    }
    return true;
  }

  // debug mode, save all ids to file
  public boolean debugGroupTofile(String groupname, String date, String filename) {
    BitmapGroup bg = getGroup(groupname);
    if (bg == null)
      return false;
    Future<Bitmap> fbm = rawOpGroup(bg, date, OPERATOR.or);
    Bitmap bm = null;
    try {
      bm = fbm.get();
    } catch (InterruptedException | ExecutionException e1) {
      e1.printStackTrace();
    }
    if (bm == null) {
      return false;
    }
    try {
      ((LocalBitmap) bm).saveToIntFile(filename);
    } catch (IOException e) {
      // ignore
    }
    return true;
  }

  // debug mode, show last update timestamp
  public long debugUpdate(String biz, String date) {
    Bitmap bm = bitmapTable.get(biz, date);
    if (bm == null) {
      return -1L;
    }
    return bm.lastUpdate();
  }
  
  public long pv(String biz, String date) {
    Bitmap bm = bitmapTable.get(biz, date);
    if (bm == null) {
      bm = loadBitmap(biz, date);
    }
    if (bm == null)
      return -1L;

    return bm.getHitCount();
  }

  public long count(String biz, String date) {
    Bitmap bm = bitmapTable.get(biz, date);
    if (bm == null) {
      bm = loadBitmap(biz, date);
    }
    if (bm == null)
      return -1L;

    return bm.count();
  }

  public long count(String biz, Date date) {
    return count(biz, formatDateToStr(date));
  }

  /**
   * force it! This is last hope
   */
  public void forceSave() {
    bitmapTable.cellSet().forEach(entry -> {
      Bitmap bm = entry.getValue();
      try {
        save(bm);
      } catch (IOException e) {
        // do nothing
      }
    });
    saveGroup();
  }

  /**
   * force to do serialize
   */
  public synchronized boolean maintain() {
    Future<Long> maintenceFuture = futureTable.get(MAINTAIN_ROW, DEFAULT_COLUMN);
    if (maintenceFuture == null) {
      maintenceFuture = maintainanceExec.submit(() -> {
        log.info("Start maintenance.");
        ArrayList<Cell<String, String, Bitmap>> list = new ArrayList<Cell<String, String, Bitmap>>();
        saveGroup();
        bitmapTable.cellSet().forEach(entry -> {
          list.add(entry);
        });
        list.forEach(entry -> {
          Bitmap bm = entry.getValue();
          try {
            if (bm.isModified())
              save(bm);
            if (!checkIfToday(bm)) {
              if ((System.currentTimeMillis() - bm.lastUpdate() > DateUtils.MILLIS_PER_HOUR)) {
                bm.close();
                bitmapTable.remove(bm.getBizname(), bm.getKeyname());
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
        log.info("End maintenance sucessfully, " + list.size() + "items processed.");
        return (long) list.size();
      });
      futureTable.put(SHUTDOWN_ROW, DEFAULT_COLUMN, maintenceFuture);
    }
    try {
      if (maintenceFuture.get(10, TimeUnit.MICROSECONDS) > 0) {
        futureTable.remove(SHUTDOWN_ROW, DEFAULT_COLUMN);
        return true;
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      return false;
    }
    return false;
  }

  public Future<Long> shutdown() throws InterruptedException, TimeoutException, ExecutionException {
    Future<Long> ret = futureTable.get(SHUTDOWN_ROW, DEFAULT_COLUMN);
    if (ret == null) {
      CountDownLatch cdl = new CountDownLatch(bitmapTable.size() + 1);
      maintainanceExec.execute(() -> {
        try {
          saveGroup();
        } finally {
          cdl.countDown();
        }
      });
      bitmapTable.cellSet().forEach(entry -> {
        Bitmap bm = entry.getValue();
        maintainanceExec.execute(() -> {
          try {
            if (!bm.isModified()) {
              bm.close();
              return;
            }
            save(bm);
            bm.close();
          } catch (IOException e) {
            // do nothing
          } finally {
            cdl.countDown();
          }
        });
      });

      Future<Long> shutdownFuture = new SelfDesFuture<Long>(new Future<Long>() {
        @Override
        public boolean cancel(boolean arg0) {
          // do nothing
          return false;
        }

        @Override
        public Long get() throws InterruptedException, ExecutionException {
          if (cdl.getCount() != 0)
            throw new InterruptedException();
          return cdl.getCount();
        }

        @Override
        public Long get(long arg0, TimeUnit arg1) throws InterruptedException, ExecutionException, TimeoutException {
          if (!cdl.await(arg0, arg1))
            throw new InterruptedException();
          return cdl.getCount();
        }

        @Override
        public boolean isCancelled() {
          // do nothing
          return false;
        }

        @Override
        public boolean isDone() {
          return cdl.getCount() == 0;
        }
      }, futureTable, SHUTDOWN_ROW, DEFAULT_COLUMN);
      futureTable.put(SHUTDOWN_ROW, DEFAULT_COLUMN, shutdownFuture);
      return shutdownFuture;
    }
    return ret;
  }

  // TODO save to remote store

  public BitMapReport status() {
    BitMapReport report = new BitMapReport();
    report.bitmaps = new HashMap<String, String>();
    bitmapTable.cellSet().forEach(entry -> {
      log.debug("get bitmap " + entry.getColumnKey() + entry.getRowKey());
      report.bitmaps.put(entry.getRowKey() + "." + entry.getColumnKey(), entry.getValue().count()
          + " counts, updated on " + entry.getValue().lastUpdate() + (entry instanceof StringBitmap ? ", type S" : ""));
      report.bitmapCount++;
    });
    report.heapSize = estimateMem();
    return report;
  }

  private boolean checkIfToday(Bitmap bm) {
    Date today = new Date();
    String sdate = formatDateToStr(today);
    if (bm.getKeyname().equals(sdate))
      return true;
    return false;
  }

  private long estimateMem() {
    Table<String, String, Bitmap> table = BitMapManager.bitmapTable;
    long sum = 0;
    for (Cell<String, String, Bitmap> cell : table.cellSet()) {
      sum += cell.getValue().getByteSize();
    }
    return sum;
  }

  class SelfDesFuture<K> implements Future<K> {

    Future<K> realFuture;
    Table<String, String, Future<K>> table;
    Object rowKey, columnKey;

    public SelfDesFuture(Future<K> future, Table<String, String, Future<K>> table, String rowKey, String columnKey) {
      this.realFuture = future;
      this.table = table;
      this.rowKey = rowKey;
      this.columnKey = columnKey;
    }

    @Override
    public boolean cancel(boolean arg0) {
      return realFuture.cancel(arg0);
    }

    @Override
    public K get() throws InterruptedException, ExecutionException {
      K result = realFuture.get();
      table.remove(rowKey, columnKey);
      return result;
    }

    @Override
    public K get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      K result = realFuture.get(timeout, unit);
      table.remove(rowKey, columnKey);
      return result;
    }

    @Override
    public boolean isCancelled() {
      return realFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
      return realFuture.isDone();
    }

  }

  public Future<HashMap<Integer, Double>> retention(String biz, String endDate, List<Integer> days, boolean nextday) {
    String futurekey = biz + "retention";
    Future<HashMap<Integer, Double>> ret = mapFutureTable.get(futurekey, endDate);
    if (ret != null) {
      return ret;
    }
    RetentionJob ba = new RetentionJob(biz, endDate, days, nextday);
    Future<HashMap<Integer, Double>> future = exec.submit(ba);
    if (!future.isDone()) {
      Future<HashMap<Integer, Double>> retfuture = new SelfDesFuture<>(future, mapFutureTable, futurekey, endDate);
      mapFutureTable.put(futurekey, endDate, retfuture);
      return retfuture;
    }
    return future;
  }

  public Future<HashMap<Integer, Double>> avgretention(String biz, String endDate, List<Integer> days) {
    String futurekey = biz + "avgretention";
    Future<HashMap<Integer, Double>> ret = mapFutureTable.get(futurekey, endDate);
    if (ret != null) {
      return ret;
    }
    RetentionJob ba = new RetentionJob(biz, endDate, days);
    Future<HashMap<Integer, Double>> future = exec.submit(ba);
    if (!future.isDone()) {
      Future<HashMap<Integer, Double>> retfuture = new SelfDesFuture<>(future, mapFutureTable, futurekey, endDate);
      mapFutureTable.put(futurekey, endDate, retfuture);
      return retfuture;
    }
    return future;
  }

  public Future<HashMap<Integer, Double>> DAU(String biz, String endDate, List<Integer> days) {
    String futurekey = biz + "DAU";
    Future<HashMap<Integer, Double>> ret = mapFutureTable.get(futurekey, endDate);
    if (ret != null) {
      return ret;
    }
    DAUJob ba = new DAUJob(biz, endDate, days);
    Future<HashMap<Integer, Double>> future = exec.submit(ba);
    if (!future.isDone()) {
      Future<HashMap<Integer, Double>> retfuture = new SelfDesFuture<>(future, mapFutureTable, futurekey, endDate);
      mapFutureTable.put(futurekey, endDate, retfuture);
      return retfuture;
    }
    return future;
  }
  
  public HashMap<Integer, Long> pv(String biz, String endDate, List<Integer> days) {
    HashMap<Integer, Long> ret = new HashMap<Integer, Long>();
    SimpleDateFormat sdf = new SimpleDateFormat(COMMON_DATE);
    Date date;
    try {
      date = sdf.parse(endDate);
    } catch (ParseException e) {
      return null;
    }
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);

    for (int intd : days) {
      calendar.add(Calendar.DAY_OF_MONTH, -intd);
      String dbefore = sdf.format(calendar.getTime());
      Bitmap dbeforebm = bitmapTable.get(biz, dbefore);
      if (dbeforebm == null) {
        dbeforebm = tryLoad(biz, dbefore);
      }
      if (dbeforebm == null)
        continue;
      long dayoneRtt = dbeforebm.getHitCount();
      ret.put(Integer.valueOf(dbefore), dayoneRtt);
      calendar.add(Calendar.DAY_OF_MONTH, intd);
    }

    return ret;
  }

  public Future<HashMap<String, Long>> listGDAU(String bgname, String endDate) {
    BitmapGroup bg = bmgroupMap.get(bgname);
    if (bg == null)
      return new ImmediateFuture<HashMap<String, Long>>(new HashMap<String, Long>());
    HashMap<String, Long> map = new HashMap<String, Long>();
    bg.biznames.forEach(name -> {
      Bitmap btm = get(name, endDate);
      if (btm != null)
        map.put(btm.getBizname(), btm.count());
    });
    return new ImmediateFuture<HashMap<String, Long>>(map);
  }

  public Future<Long> opCount(String biz, Date d1, Date d2, OPERATOR op) {
    String sd1 = formatDateToStr(d1);
    String sd2 = formatDateToStr(d2);
    return opCount(biz, sd1, sd2, op);
  }

  public Future<Long> opCount(String biz, String sd1, String sd2, OPERATOR op) {
    return opCount(biz, sd1, biz, sd2, op);
  }

  public Future<Long> opCount(String biz1, String sd1, String biz2, String sd2, OPERATOR op) {
    String futurekey = biz1 + biz2 + op;
    String futurekeyD = sd1 + sd2;
    Future<Long> ret = futureTable.get(futurekey, futurekeyD);
    if (ret != null) {
      return ret;
    }

    Bitmap bm1 = bitmapTable.get(biz1, sd1);
    if (bm1 == null) {
      bm1 = tryLoad(biz1, sd1);
    }
    Bitmap bm2 = bitmapTable.get(biz2, sd2);
    if (bm2 == null) {
      bm2 = tryLoad(biz2, sd2);
    }

    BitmapOpCounter ba = new BitmapOpCounter(new Bitmap[] { bm1, bm2 }, op);
    Future<Long> future = exec.submit(ba);
    if (!future.isDone()) {
      Future<Long> retfuture = new SelfDesFuture<>(future, futureTable, futurekey, futurekeyD);
      futureTable.put(futurekey, futurekeyD, retfuture);
      return retfuture;
    }
    return future;
  }

  public Future<Bitmap> rawOp(Bitmap bm1, Bitmap bm2, OPERATOR op) {
    if (bm1 == null || bm2 == null) {
      return new ImmediateFuture<Bitmap>(EmptyLocalBitmap.getInstance());
    }
    BitmapRawOperator ba = new BitmapRawOperator(new Bitmap[] { bm1, bm2 }, op);
    Future<Bitmap> future = exec.submit(ba);
    if (!future.isDone()) {
      String futurekey = "raw" + bm1.getBizname() + bm2.getBizname();
      String futurekeyD = "raw" + bm1.getKeyname() + bm2.getKeyname();
      Future<Bitmap> retfuture = new SelfDesFuture<>(future, rawFutureTable, futurekey, futurekeyD);
      rawFutureTable.put(futurekey, futurekeyD, retfuture);
      return retfuture;
    }
    return future;
  }

  public Future<Bitmap> rawOp(Bitmap[] bms, OPERATOR op) {
    for (int i = 0; i < bms.length; i++) {
      if (bms[i] == null) {
        return new ImmediateFuture<Bitmap>(EmptyLocalBitmap.getInstance());
      }
    }
    BitmapRawOperator ba = new BitmapRawOperator(bms, op);
    Future<Bitmap> future = exec.submit(ba);
    if (!future.isDone()) {
      String futurekey = "raw" + bms[0].getBizname() + bms[bms.length - 1].getBizname();
      String futurekeyD = "raw" + bms[0].getKeyname() + bms[bms.length - 1].getKeyname();
      Future<Bitmap> retfuture = new SelfDesFuture<>(future, rawFutureTable, futurekey, futurekeyD);
      rawFutureTable.put(futurekey, futurekeyD, retfuture);
      return retfuture;
    }
    return future;
  }

  private boolean checkValidDate(String date) {
    if (date == null)
      return false;
    if (date.length() != 8)
      return false;
    return true;
  }

  public Bitmap get(String bizname, String date) {
    Bitmap ret = bitmapTable.get(bizname, date);
    if (ret == null) {
      return tryLoad(bizname, date);
    }
    return ret;
  }

  public Future<String> express(String expression) {
    Object result = AviatorEvaluator.execute(expression);
    if (result == null) {
      return new ImmediateFuture<String>(String.valueOf(0L));
    } else if (result instanceof Bitmap) {
      Bitmap ba = (Bitmap) AviatorEvaluator.execute(expression);
      return new ImmediateFuture<String>(String.valueOf(ba.count()));
    } else {
      // direct value
      return new ImmediateFuture<String>(String.valueOf(result));
    }
  }

  public Future<Bitmap> rawOpGroup(BitmapGroup bg, String date, OPERATOR op) {
    if (bg == null || !checkValidDate(date)) {
      return new ImmediateFuture<Bitmap>(EmptyLocalBitmap.getInstance());
    }
    String[] biznames = bg.getBiznames();
    String[] dates = new String[biznames.length];
    for (int i = 0; i < dates.length; i++) {
      dates[i] = date;
    }
    ArrayList<Bitmap> list = new ArrayList<Bitmap>();
    for (int i = 0; i < biznames.length; i++) {
      Bitmap bm = bitmapTable.get(biznames[i], date);
      if (bm == null) {
        bm = tryLoad(biznames[i], date);
      }
      if (bm != null)
        list.add(bm);
    }
    Bitmap[] bmarr = list.stream().toArray(Bitmap[]::new);
    BitmapRawOperator ba = new BitmapRawOperator(bmarr, op);
    Future<Bitmap> future = exec.submit(ba);
    if (!future.isDone()) {
      String futurekey = "rawG" + bg.name;
      String futurekeyD = "rawG" + date;
      Future<Bitmap> retfuture = new SelfDesFuture<>(future, rawFutureTable, futurekey, futurekeyD);
      rawFutureTable.put(futurekey, futurekeyD, retfuture);
      return retfuture;
    }
    return future;
  }

  private String hash(String[] strs) {
    long result = 17;
    for (String v : strs)
      result = 37 * result + v.hashCode();
    return String.valueOf(result);
  }

  public Future<Long> opCount(String[] bizs, String[] sds, OPERATOR op) {
    if (bizs.length != sds.length)
      return new ImmediateFuture<Long>(0L);
    String futurekey = hash(bizs) + op;
    String futurekeyD = hash(sds) + op;
    Future<Long> ret = futureTable.get(futurekey, futurekeyD);
    if (ret != null) {
      return ret;
    }

    ArrayList<Bitmap> list = new ArrayList<Bitmap>();

    for (int i = 0; i < sds.length; i++) {
      Bitmap bm = bitmapTable.get(bizs[i], sds[i]);
      if (bm == null) {
        bm = tryLoad(bizs[i], sds[i]);
      }
      if (bm != null)
        list.add(bm);
    }

    Bitmap[] bmarr = list.stream().toArray(Bitmap[]::new);
    BitmapOpCounter ba = new BitmapOpCounter(bmarr, op);
    Future<Long> future = exec.submit(ba);
    if (!future.isDone()) {
      Future<Long> retfuture = new SelfDesFuture<>(future, futureTable, futurekey, futurekeyD);
      futureTable.put(futurekey, futurekeyD, retfuture);
      return retfuture;
    }
    return future;
  }

  public Future<Long> countGroup(String bgname, String date, OPERATOR op) {
    BitmapGroup bg = this.bmgroupMap.get(bgname);
    if (bg == null || date == null || date.length() != 8) {
      return new ImmediateFuture<Long>(0L);
    }
    String[] biznames = bg.getBiznames();
    String[] dates = new String[biznames.length];
    for (int i = 0; i < dates.length; i++) {
      dates[i] = date;
    }
    return opCount(biznames, dates, op);
  }

  public Future<HashMap<Integer, Double>> countGroup(String bgname, String[] date, String op) {
    if (bgname == null || date == null) {
      return new ImmediateFuture<HashMap<Integer, Double>>(null);
    }
    String futurekey = bgname + "gDAU";
    Future<HashMap<Integer, Double>> ret = mapFutureTable.get(futurekey, date[0] + date[date.length - 1]);
    if (ret != null) {
      return ret;
    }

    List<Integer> days = new ArrayList<Integer>();
    for (int i = 0; i < date.length; i++) {
      days.add(Integer.parseInt(date[i]));
    }
    GDAUJob ba = new GDAUJob(bgname, days, op);
    Future<HashMap<Integer, Double>> future = exec.submit(ba);
    if (!future.isDone()) {
      Future<HashMap<Integer, Double>> retfuture = new SelfDesFuture<>(future, mapFutureTable, futurekey,
          date[0] + date[date.length - 1]);
      mapFutureTable.put(futurekey, date[0] + date[date.length - 1], retfuture);
      return retfuture;
    }
    return future;
  }

  /**
   * load the bitmap If the localfile does not exist, it will try to load from the
   * oss storage
   * 
   */
  private Bitmap tryLoad(String biz, String sd) {
    Bitmap bm = loadBitmap(biz, sd);
    return bm;
  }

  private String getLocalFilename(String biz, String date) {
    return "/data/bitmap/store/"+date+"/"+biz + "_" + date + ".btm";
  }

  private String getossname(String biz, String date) {
    return biz + "_" + date;
  }

  public void clean(String biz, Date date) {
    Bitmap bm = bitmapTable.get(biz, formatDateToStr(date));
    if (bm == null) {
      return;
    }
    try {
      bm.close();
    } catch (IOException e) {
      // do nothing
    }
  }

  public boolean existGroup(String groupname) {
    if (groupname == null || groupname.length() == 0)
      return false;
    if (bmgroupMap.get(groupname) != null)
      return true;
    return false;
  }

  public BitmapGroup getGroup(String groupname) {
    if (groupname == null || groupname.length() == 0)
      return null;
    if (bmgroupMap.get(groupname) != null)
      return bmgroupMap.get(groupname);
    BitmapGroup bg = new BitmapGroup(groupname);
    bmgroupMap.put(groupname, bg);
    return bg;
  }

  public HashMap<String, Integer> getGroups() {
    final HashMap<String, Integer> ret = new HashMap<String, Integer>();
    bmgroupMap.entrySet().forEach(ent -> {
      if (ent != null)
        ret.put(ent.getKey(), ent.getValue().getSize());
    });
    return ret;
  }

  public BitmapGroup removeFromGroup(String groupname, String[] bizname) {
    if (groupname == null || groupname.length() == 0)
      return null;
    BitmapGroup bg = getGroup(groupname); // bmgroupMap.get(groupname);
    if (bg == null) {
      bg = new BitmapGroup(groupname);
      bmgroupMap.put(groupname, bg);
    }
    for (int i = 0; i < bizname.length; i++) {
      bg.remove(bizname[i]);
    }
    return bg;
  }
  
  public BitmapGroup addToGroup(String groupname, String[] bizname) {
    if (groupname == null || groupname.length() == 0)
      return null;
    BitmapGroup bg = getGroup(groupname); // bmgroupMap.get(groupname);
    if (bg == null) {
      bg = new BitmapGroup(groupname);
      bmgroupMap.put(groupname, bg);
    }
    for (int i = 0; i < bizname.length; i++) {
      bg.add(bizname[i]);
    }
    return bg;
  }

  public BitmapGroup addToGroupWithPrefix(String groupname, String prefix) {
    if (groupname == null || groupname.length() == 0)
      return null;
    final BitmapGroup bg = getGroup(groupname);// bmgroupMap.get(groupname) == null ? new BitmapGroup(groupname) :
                                               // bmgroupMap.get(groupname);

    bitmapTable.cellSet().forEach(consumer -> {
      String key = consumer.getRowKey();
      if (key.startsWith(prefix))
        bg.add(key);
    });
    bmgroupMap.put(groupname, bg);
    return bg;
  }

  class BitmapOpCounter implements Callable<Long> {
    Bitmap[] bms;
    OPERATOR op;

    public BitmapOpCounter(Bitmap[] bms, OPERATOR op) {
      this.bms = bms;
      this.op = op;
    }

    @Override
    public Long call() throws Exception {
      Bitmap res = null;
      for (int i = 1; i < bms.length; i++) {
        if (bms[i] == null)
          return -1L;
        if (bms[i].getStatus() == Bitmap.STATUS.INFILE)
          ready(bms[i]);
      }
      switch (op) {
      case and:
        res = LocalBitmap.AND(bms);
        break;
      case or:
        res = LocalBitmap.OR(bms);
        break;
      case xor:
        res = LocalBitmap.XOR(bms);
        break;
      }
      if (res == null) {
        return -1L;
      }
      return res.count();
    }

    private Bitmap ready(Bitmap b) {
      return b.deSerialize(null);
    }
  } // end of class BitmapAndCounter

  class BitmapRawOperator implements Callable<Bitmap> {
    Bitmap[] bms;
    OPERATOR op;

    public BitmapRawOperator(Bitmap[] bms, OPERATOR op) {
      this.bms = bms;
      this.op = op;
    }

    @Override
    public Bitmap call() throws Exception {
      Bitmap res = null;
      for (int i = 1; i < bms.length; i++) {
        if (bms[i] == null)
          return EmptyLocalBitmap.getInstance();
        if (bms[i].getStatus() == Bitmap.STATUS.INFILE)
          ready(bms[i]);
      }
      switch (op) {
      case and:
        res = LocalBitmap.AND(bms);
        break;
      case or:
        res = LocalBitmap.OR(bms);
        break;
      case xor:
        res = LocalBitmap.XOR(bms);
        break;
      case andnot:
        res = LocalBitmap.ANDNOT(bms);
        break;
      }
      if (res == null) {
        return EmptyLocalBitmap.getInstance();
      }
      return ((ImmutableBitmap) res).getRealMap();
    }

    private Bitmap ready(Bitmap b) {
      return b.deSerialize(null);
    }
  } // end of class BitmapRawOperator

  class RetentionJob implements Callable<HashMap<Integer, Double>> {
    String biz;
    String endDate;
    List<Integer> days;
    boolean nextday;

    public RetentionJob(String biz, String endDate, List<Integer> days) {
      this.biz = biz;
      this.endDate = endDate;
      this.days = days;
    }

    public RetentionJob(String biz, String endDate, List<Integer> days, boolean nextday) {
      this.biz = biz;
      this.endDate = endDate;
      this.days = days;
      this.nextday = nextday;
    }

    @Override
    public HashMap<Integer, Double> call() throws Exception {
      SimpleDateFormat sdf = new SimpleDateFormat(COMMON_DATE);
      Date date;
      try {
        date = sdf.parse(endDate);
      } catch (ParseException e) {
        return null;
      }
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      Calendar calendarbefore = Calendar.getInstance();
      calendarbefore.setTime(date);

      Bitmap bm = bitmapTable.get(biz, endDate);
      if (bm == null) {
        bm = tryLoad(biz, endDate);
      }
      if (bm == null)
        return null;

      HashMap<Integer, Double> retMap = new HashMap<Integer, Double>();
      if (nextday) {
        for (int intd : days) {
          calendar.add(Calendar.DAY_OF_MONTH, -intd);
          calendarbefore.add(Calendar.DAY_OF_MONTH, -intd - 1);
          String sday = sdf.format(calendar.getTime());
          Bitmap daybm = bitmapTable.get(biz, sday);
          String sbeforeday = sdf.format(calendarbefore.getTime());
          Bitmap beforebm = bitmapTable.get(biz, sbeforeday);
          if (daybm == null) {
            daybm = tryLoad(biz, sday);
          }
          if (beforebm == null) {
            beforebm = tryLoad(biz, sbeforeday);
          }
          if (beforebm == null || daybm == null)
            continue;
          double dayoneRtt = LocalBitmap.AND(daybm, beforebm).count() * 1.0 / beforebm.count();
          retMap.put(Integer.valueOf(sday), dayoneRtt);
          calendar.add(Calendar.DAY_OF_MONTH, intd);
        }
      } else {
        for (int intd : days) {
          calendar.add(Calendar.DAY_OF_MONTH, -intd);
          String dbefore = sdf.format(calendar.getTime());
          Bitmap dbeforebm = bitmapTable.get(biz, dbefore);
          if (dbeforebm == null) {
            dbeforebm = tryLoad(biz, dbefore);
          }
          if (dbeforebm == null)
            continue;
          double dayoneRtt = LocalBitmap.AND(bm, dbeforebm).count() * 1.0 / dbeforebm.count();
          retMap.put(Integer.valueOf(dbefore), dayoneRtt);
          calendar.add(Calendar.DAY_OF_MONTH, intd);
        }
      }

      return retMap;
    }
  } // end of class RetentionJob

  class ImmediateFuture<T> implements Future<T> {

    T value;

    public ImmediateFuture(T value) {
      this.value = value;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      return value;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return value;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public String toString() {
      return value.toString();
    }
  }

  class DAUJob implements Callable<HashMap<Integer, Double>> {
    String biz;
    String endDate;
    List<Integer> days;

    public DAUJob(String biz, String endDate, List<Integer> days) {
      this.biz = biz;
      this.endDate = endDate;
      this.days = days;
    }

    @Override
    public HashMap<Integer, Double> call() throws Exception {
      SimpleDateFormat sdf = new SimpleDateFormat(COMMON_DATE);
      Date date;
      try {
        date = sdf.parse(endDate);
      } catch (ParseException e) {
        return null;
      }
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);

      Bitmap bm = bitmapTable.get(biz, endDate);
      if (bm == null) {
        bm = tryLoad(biz, endDate);
      }
      if (bm == null)
        return null;

      HashMap<Integer, Double> retMap = new HashMap<Integer, Double>();
      for (int intd : days) {
        calendar.add(Calendar.DAY_OF_MONTH, -intd);
        String dbefore = sdf.format(calendar.getTime());
        Bitmap dbeforebm = bitmapTable.get(biz, dbefore);
        if (dbeforebm == null) {
          dbeforebm = tryLoad(biz, dbefore);
        }
        if (dbeforebm == null)
          continue;
        long dayoneRtt = dbeforebm.count();
        retMap.put(Integer.valueOf(dbefore), new Long(dayoneRtt).doubleValue());
        calendar.add(Calendar.DAY_OF_MONTH, intd);
      }

      return retMap;
    }
  } // end of class DAUJob

  class GDAUJob implements Callable<HashMap<Integer, Double>> {
    String bgname;
    List<Integer> days;
    OPERATOR op;

    public GDAUJob(String bgname, List<Integer> days, String operator) {
      this.bgname = bgname;
      this.days = days;
      switch (operator) {
      case "and":
        op = OPERATOR.and;
        break;
      case "xor":
        op = OPERATOR.xor;
        break;
      case "or":
      default:
        op = OPERATOR.or;
        break;
      }
    }

    @Override
    public HashMap<Integer, Double> call() throws Exception {
      final HashMap<Integer, Bitmap[]> map = new HashMap<Integer, Bitmap[]>();
      BitmapGroup bg = getGroup(bgname);
      days.forEach(day -> {
        Bitmap[] bms = new Bitmap[bg.biznames.size()];
        map.put(day, bms);
        int[] i = new int[1];
        i[0] = 0;
        bg.biznames.forEach(btmname -> {
          Bitmap bm = bitmapTable.get(btmname, String.valueOf(day));
          if (bm == null) {
            bm = tryLoad(btmname, String.valueOf(day));
          }
          if (bm == null) {
            bm = EmptyLocalBitmap.getInstance();
          }
          bms[i[0]] = bm;
          i[0]++;
        });
      });

      Bitmap tmp = null;
      HashMap<Integer, Double> retMap = new HashMap<Integer, Double>();
      for (int intd : days) {
        Bitmap[] bms = map.get(intd);
        if (bms.length <= 0) {
          log.error(bg.name + ":" + bg.biznames.size() + ", days =" + days.size());
        }
        switch (op) {
        case and:
          tmp = LocalBitmap.AND(bms);
          break;
        case or:
          tmp = LocalBitmap.OR(bms);
          break;
        case xor:
          tmp = LocalBitmap.XOR(bms);
          break;
        }
        if (tmp != null) {
          Long v = tmp.count();
          retMap.put(intd, v.doubleValue());
        }
      }

      return retMap;
    }
  } // end of class GDAUJob
}
