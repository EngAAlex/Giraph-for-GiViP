/**
 * 
 */
package org.apache.giraph.profiler;

import com.google.protobuf.GeneratedMessage;
import com.unipg.profilercommon.protoutils.ExecutedSuperstepWorkerInfoProto;
import com.unipg.profilercommon.protoutils.ExecutedSuperstepWorkerInfoProto.ExecSuperstepWorkerInfo.SuperstepInfo;

import java.io.IOException;

import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.fs.Path;
/**
 * @author maria
 *
 */
public class ExecutedSuperstepWorkerInfo extends Writer {

  private String directory = "profiler";
  private String fileName = "ExecutedSuperstepWorkerInfo";
  public String jobId = null;  
  private long currentSuperstep;

  private ExecutedSuperstepWorkerInfoProto.ExecSuperstepWorkerInfo.Builder infoBuilder;
  private SuperstepInfo.Builder superstepBuilder;
  private WorkerInfo worker;

  
  /**
   * @throws
   */  
  public ExecutedSuperstepWorkerInfo(WorkerInfo worker,String jobId){
    this.jobId = jobId;
    this.worker = worker;
    this.infoBuilder = ExecutedSuperstepWorkerInfoProto.ExecSuperstepWorkerInfo.newBuilder();
    this.superstepBuilder = SuperstepInfo.newBuilder();
    this.infoBuilder.setWorker(this.worker.getTaskId());
  }
  
  
  /**
   * @param computName 
   * @param computMillis
   * @param gcMillis
   * @param waitMillis
   * @param prVertices
   * @param nPartitions
   * @throws IOException
   */
  public void newSuperstepInfo(String computName, long computMillis, long prVertices, int nPartitions){
    this.superstepBuilder.setComputationName(computName);
    this.superstepBuilder.setSuperstepIndex(this.currentSuperstep);
    this.superstepBuilder.setComputationSecs(computMillis);
    this.superstepBuilder.setProcessedVertices(prVertices);
    this.superstepBuilder.setNOfPartitions(nPartitions);
    
    this.infoBuilder.addSuperstepInfo(this.superstepBuilder.build());
    }  
  
  public void updateSuperstep(long superstep){
    this.currentSuperstep = superstep;
  } 
  
  /* (non-Javadoc)
   * @see org.apache.giraph.profiler.Writer#generateMessageToWrite()
   */
  @Override
  public GeneratedMessage generateMessageToWrite() {
    return this.infoBuilder.build();
  }

  /* (non-Javadoc)
   * @see org.apache.giraph.profiler.Writer#generateFullPath()
   */
  @Override
  public String generateFullPath() {
    return 
        this.directory + Path.SEPARATOR  
        + this.jobId + Path.SEPARATOR + "WorkerSuperstepInfo"+ Path.SEPARATOR
        +this.fileName+"WorkerN-"+this.worker.getTaskId();
  }

  /* (non-Javadoc)
   * @see org.apache.giraph.profiler.Writer#clearMessages()
   */
  @Override
  public void clearMessages() {
    this.infoBuilder.clear();
  }

  /* (non-Javadoc)
   * @see org.apache.giraph.profiler.Writer#countMessages()
   */
  @Override
  public int countMessages() {
    return this.infoBuilder.getSuperstepInfoCount();
  }

}
