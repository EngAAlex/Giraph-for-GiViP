/**
 * 
 */
package org.apache.giraph.givip.profiler;

import java.util.HashSet;

import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.Node;

import com.google.protobuf.GeneratedMessage;
import com.unipg.givip.common.protoutils.HierarchyProtoBook.SingleWorkerTreeHierarchy;
import com.unipg.givip.common.protoutils.HierarchyProtoBook.WorkerHierarchy;

/**
 * @author maria
 *
 */
public class HierarchyWrapper extends Writer{
  private SingleWorkerTreeHierarchy.Builder singleWorkerHierarchyBuilder= null;
  private WorkerHierarchy.Builder workerHierarchyBook = null;
  
  private String jobId;
  private String fileName = "JobHierarchy";
  private String directory = "profiler";
  
  private HashSet<String> hostnames; 

  public HierarchyWrapper(Configuration conf){
    super(conf);
//    try {
//      this.fileSystem = FileSystem.get(new Configuration());
//    } catch (IOException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
    this.workerHierarchyBook = WorkerHierarchy.newBuilder();
    this.singleWorkerHierarchyBuilder = SingleWorkerTreeHierarchy.newBuilder();
    this.jobId = conf.get("mapred.job.id", "Unknown Job");
    this.hostnames = new HashSet<String>();
    
  }

    public void saveNodeHierarchy(Node node, WorkerInfo winfo){
      String hostName = node.getName();
      
      hostnames.add(hostName);
      
      String rack = node.getNetworkLocation();
      this.singleWorkerHierarchyBuilder.setHostName(hostName);
      this.singleWorkerHierarchyBuilder.setRack(rack);
      this.singleWorkerHierarchyBuilder.setWorker(winfo.getTaskId());
      
      this.workerHierarchyBook.addSingleWorkerTreeHierarchy(this.singleWorkerHierarchyBuilder.build());
  
    }

    public GeneratedMessage generateRecordToWrite(){
      return this.workerHierarchyBook.build();
    }
    
    public void clearMessages(){
      this.workerHierarchyBook.clear();
    }
    
    public String generateFullPath(){
      return
          this.directory + Path.SEPARATOR 
          + this.jobId+ Path.SEPARATOR 
          + this.fileName;
    }
    
    public int countMessages(){
      return this.workerHierarchyBook.build().getSingleWorkerTreeHierarchyCount();
    }

	public HashSet<String> getHostnames() {
		return hostnames;
	}
    

}


