/**
 * 
 */
package org.apache.giraph.profiler;

import com.unipg.profilercommon.protoutils.HierarchyProtoBook.SingleWorkerTreeHierarchy;
import com.unipg.profilercommon.protoutils.HierarchyProtoBook.WorkerHierarchy;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.Node;

import com.google.protobuf.GeneratedMessage;

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

  public HierarchyWrapper(String jobId){
    super();
//    try {
//      this.fileSystem = FileSystem.get(new Configuration());
//    } catch (IOException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
    this.workerHierarchyBook = WorkerHierarchy.newBuilder();
    this.singleWorkerHierarchyBuilder = SingleWorkerTreeHierarchy.newBuilder();
    this.jobId = jobId;
    
  }

    public void saveNodeHierarchy(Node node, WorkerInfo winfo){
      String hostName = node.getName();
      String rack = node.getNetworkLocation();
      this.singleWorkerHierarchyBuilder.setHostName(hostName);
      this.singleWorkerHierarchyBuilder.setRack(rack);
      this.singleWorkerHierarchyBuilder.setWorker(winfo.getTaskId());
      
      this.workerHierarchyBook.addSingleWorkerTreeHierarchy(this.singleWorkerHierarchyBuilder.build());
  
    }

    public GeneratedMessage generateMessageToWrite(){
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

}


