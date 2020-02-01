package lv.jing.taichi.core.provider;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lv.jing.taichi.core.conf.HDFSConf;
import lv.jing.taichi.core.conf.TaichiConf;

public class HDFSProvider {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  Configuration conf = new Configuration();

  @Inject
  public HDFSProvider(TaichiConf tconf) {
    HDFSConf cn = tconf.getHDFS();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    conf = new Configuration();
//    String path = cn.getCorePath();
//    conf.set("fs.defaultFS", path);
  }

  /**
   * Get file list from the
   *
   * @param srcpath
   * @return
   */
  public String[] getFileList(String srcpath) {
    try {
      Path path = new Path(srcpath);
      FileSystem fs = path.getFileSystem(conf);
      List<String> files = new ArrayList<String>();
      if (fs.exists(path) && fs.isDirectory(path)) {
        for (FileStatus status : fs.listStatus(path)) {
          files.add(status.getPath().toString());
        }
      }
      return files.toArray(new String[] {});
    } catch (Exception e2) {
      // do nothing
    }
    return null;
  }

  /**
   * Write to hdfs file
   *
   * @param dst
   * @param contents
   * @throws IOException
   */
  public void createFile(String dst, byte[] contents) throws IOException {
    Path dstPath = new Path(dst);
    FileSystem fs = dstPath.getFileSystem(conf);

    FSDataOutputStream outputStream = fs.create(dstPath);
    outputStream.write(contents);
    outputStream.close();
  }

  /**
   * Delete file on hdfs
   *
   * @param filePath
   * @throws IOException
   */
  public void delete(String filePath) throws IOException {
    Path path = new Path(filePath);
    FileSystem fs = path.getFileSystem(conf);

    boolean isok = fs.deleteOnExit(path);
    if (isok) {
      System.out.println("delete file " + filePath + " success!");
    } else {
      System.out.println("delete file " + filePath + " failure");
    }
    // fs.close();
  }

  /**
   * Create a directory in hdfs
   *
   * @param path
   * @throws IOException
   */
  public boolean mkdir(String path) throws IOException {
    Path srcPath = new Path(path);
    FileSystem fs = srcPath.getFileSystem(conf);

    return fs.mkdirs(srcPath);
  }

  /**
   * Load file to local in certain directory
   *
   * @param dstPath
   * @param srcPath
   * @throws Exception
   */
  public void downloadFile(String dstPath, String srcPath, boolean recurse) throws Exception {
    Path path = new Path(srcPath);
    FileSystem hdfs = path.getFileSystem(conf);

    File rootfile = new File(dstPath);
    if (!rootfile.exists()) {
      rootfile.mkdirs();
    }

    if (hdfs.isFile(path)) {
      String fileName = path.getName();
      if (!fileName.toLowerCase().endsWith(".btm")) {
        FSDataInputStream in = null;
        FileOutputStream out = null;
        try {
          in = hdfs.open(path);
          File srcfile = new File(rootfile, path.getName());
          if (!srcfile.exists())
            srcfile.createNewFile();
          out = new FileOutputStream(srcfile);
          IOUtils.copyBytes(in, out, 4096, false);
        } finally {
          IOUtils.closeStream(in);
          IOUtils.closeStream(out);
        }
      }
    } else if (hdfs.isDirectory(path) && recurse) {
      File dstDir = new File(dstPath);
      if (!dstDir.exists()) {
        dstDir.mkdirs();
      }
      String filePath = path.toString();
      String subPath[] = filePath.split("/");
      String newdstPath = dstPath + subPath[subPath.length - 1] + "/";
      if (hdfs.exists(path) && hdfs.isDirectory(path)) {
        FileStatus[] srcFileStatus = hdfs.listStatus(path);
        if (srcFileStatus != null) {
          for (FileStatus status : hdfs.listStatus(path)) {
            downloadFile(newdstPath, status.getPath().toString(), recurse);
          }
        }
      }
    }
  }

  /**
   * read the file on hdfs Note file may be very big, do load local file instead
   *
   */
  public byte[] readFileByte(String srcPath) throws Exception {
    Path path = new Path(srcPath);
    FileSystem hdfs = null;
    hdfs = FileSystem.get(URI.create(srcPath), conf);
    if (hdfs.exists(path) && hdfs.isFile(path)) {
      FSDataInputStream in = null;
      FileOutputStream out = null;
      try {
        in = hdfs.open(new Path(srcPath));
        byte[] t = new byte[in.available()];
        in.read(t);
        return t;
      } finally {
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
      }
    }
    return null;
  }

  /**
   * upload file or directory to hdfs
   *
   * @param localSrc
   * @param dst
   * @throws Exception
   */
  public void uploadFile(String localSrc, String dst) throws Exception {
    File srcFile = new File(localSrc);
    if (srcFile.isDirectory()) {
      copyDirectory(localSrc, dst, conf);
    } else {
      copyFile(localSrc, dst, conf);
    }
  }

  /**
   * upload file implementation
   * 
   */
  private boolean copyFile(String localSrc, String dst, Configuration conf) throws Exception {
    File file = new File(localSrc);
    dst = dst + file.getName();
    Path path = new Path(dst);
    FileSystem fs = path.getFileSystem(conf);
    fs.exists(path);
    InputStream in = new BufferedInputStream(new FileInputStream(file));
    OutputStream out = fs.create(new Path(dst));
    IOUtils.copyBytes(in, out, 4096, true);
    in.close();
    return true;
  }

  /**
   * upload directory implementation
   * 
   */
  private boolean copyDirectory(String src, String dst, Configuration conf) throws Exception {
    Path path = new Path(dst);
    FileSystem fs = path.getFileSystem(conf);
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
    File file = new File(src);

    File[] files = file.listFiles();
    if (files != null)
      for (int i = 0; i < files.length; i++) {
        File f = files[i];
        if (f.isDirectory()) {
          String fname = f.getName();
          if (dst.endsWith("/")) {
            copyDirectory(f.getPath(), dst + fname + "/", conf);
          } else {
            copyDirectory(f.getPath(), dst + "/" + fname + "/", conf);
          }
        } else {
          copyFile(f.getPath(), dst, conf);
        }
      }
    return true;
  }
}
