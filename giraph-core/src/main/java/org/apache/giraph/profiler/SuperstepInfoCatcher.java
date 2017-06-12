/**
 * 
 */
package org.apache.giraph.profiler;


import com.unipg.profilercommon.protoutils.SuperstepProtoInfo.Superstep;
import com.unipg.profilercommon.protoutils.SuperstepProtoInfo.SuperstepsInfo;
import org.apache.hadoop.fs.Path;

import com.google.protobuf.GeneratedMessage;

/**
 * @author maria
 *
 */
public class SuperstepInfoCatcher extends Writer{
  private Superstep.Builder superstepBuilder;
  private SuperstepsInfo.Builder jobSuperstepsInfoBuilder;
  
  private String directory = "profiler";
  private String fileName = "SuperstepsTimes";
  private String jobId;

  public SuperstepInfoCatcher (String jobId){ 
    super();
    this.jobId = jobId;
    this.jobSuperstepsInfoBuilder = SuperstepsInfo.newBuilder();
    this.superstepBuilder = Superstep.newBuilder();
  }
  
  public void addSuperstepInfo(long number, double duration){
    this.superstepBuilder.setNumero(number);
    this.superstepBuilder.setDuration(duration);
    this.jobSuperstepsInfoBuilder.addSuperstep(superstepBuilder.build());
  }
  
  public GeneratedMessage generateMessageToWrite(){
    return this.jobSuperstepsInfoBuilder.build();
  }
  
  public String generateFullPath(){
    return 
        this.directory
        + Path.SEPARATOR_CHAR  
        + this.jobId + Path.SEPARATOR_CHAR 
        + this.fileName ;
  }
  
  public void clearMessages(){
    this.jobSuperstepsInfoBuilder.clear();
  }
  
  public int countMessages(){
    return this.jobSuperstepsInfoBuilder.build().getSuperstepCount();
  }

}
