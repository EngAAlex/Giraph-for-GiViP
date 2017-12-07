package org.apache.giraph.givip.profiler;
/**
 * 
 */


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * @author maria
 *
 * Copia dei file in locale sui worker
 */
public class Copier {
	
//	Configuration conf;

  public static void copyFromLocalToHDFS(Configuration conf, String localSrc, String folder, String jobId) throws IllegalArgumentException, IOException{

    FileSystem fs = null;
    String dst = "";

    try {
      fs = FileSystem.get(conf);
      dst = fs.getHomeDirectory()+ Path.SEPARATOR 
          +"profiler" + Path.SEPARATOR 
          + jobId + Path.SEPARATOR
          + folder;
      FileSystem.mkdirs(fs, new Path(dst), FsPermission.getDefault());
    } catch (IOException e) {
      e.printStackTrace();
    }
        
    try {
      if(fs != null){
    	  fs.copyFromLocalFile(true, new Path(localSrc), new Path(dst));
      }
    } catch (IOException  e) {
    	e.printStackTrace();
    }
  }
  
}
