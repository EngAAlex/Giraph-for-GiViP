package org.apache.giraph.profiler;

import java.io.IOException;

import com.unipg.profilercommon.protoutils.MessagesProtoBook.ExchangedMessage;
import com.unipg.profilercommon.protoutils.MessagesProtoBook.MessagesBook;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.fs.Path;

import com.google.protobuf.GeneratedMessage;

/**
 * @author maria
 *
 */
public class MessagesSniffer extends Writer{

  private String directory = "profiler";
  private String fileName = "ExchangedMessages";
  public String jobId = null;

  private MessagesBook.Builder messagesBook;
  private ExchangedMessage.Builder exchangedMessages;
  private long currentSuperstep;
  private WorkerInfo worker;
  
  /**
   * @throws
   */
  public MessagesSniffer(WorkerInfo worker, String jobId) {
    super();
    this.jobId = jobId;
    this.worker = worker;
    this.messagesBook = MessagesBook.newBuilder();
    this.exchangedMessages = ExchangedMessage.newBuilder();
  }

  /**
   * @param workerSourceTaskId
   * @param workerDestTaskId
   * @param messageSize
   * @param isInternal
   * @throws IOException
   */
  public void newExchangedMessage(
    int workerSourceTaskId, int workerDestTaskId, int messagesNumber, double messageSize,
    boolean isInternal) throws IOException {
    this.exchangedMessages.setIsInternalMsg(isInternal);
    this.exchangedMessages.setWorkerDestId(workerDestTaskId);
    this.exchangedMessages.setWorkerSourceId(workerSourceTaskId);
    this.exchangedMessages.setMessagesNumber(messagesNumber);
    this.exchangedMessages.setMessagesByteSize(messageSize);

    //Add an singleExchangedMessage between
    this.messagesBook.addExchangeMessage(this.exchangedMessages.build());
  }

  public void updateSuperstep(long superstep){
    this.currentSuperstep = superstep;
  }
    
  public GeneratedMessage generateMessageToWrite(){
    return this.messagesBook.build();
  }
  
  public String generateFullPath(){
    return 
        this.directory + Path.SEPARATOR  
        + this.jobId + Path.SEPARATOR + "WorkerData"+ Path.SEPARATOR
        +"WorkerN-"+this.worker.getTaskId() + Path.SEPARATOR
        +this.fileName
        +"SuperstepN-" + this.currentSuperstep;
  }
  
  public void clearMessages(){
    this.messagesBook.clear();
  }
  
  public int countMessages(){
    return this.messagesBook.build().getExchangeMessageCount();
  }

  
///**
//* @param message
//*/
//private void addMessages(ExchangedMessage message) {
//
// // Add exchanged messages between a pair of worker in the current
// // exchanged messages superstep list
// this.messagesBook.addExchangeMessage(message);
//
//}

///**
//* Write messages book locally
//* 
//* @param superstep
//* @param worker
//* @param jobId
//* @throws IOException
//*/
//public void localFlush(long superstep, WorkerInfo worker, String jobId){
// @SuppressWarnings("unused")
// boolean success =
//   (new File(
//     System.getProperty("user.home") + "/Profiler/WrappedData/" + jobId))
//       .mkdirs();
// String localFile =
//   "WorkerTaskId"
//     + worker.getTaskId() + this.fileName + "SuperstepN" + superstep;
//
// FileOutputStream output=null;
// try {
//   output = new FileOutputStream(
//     System.getProperty("user.home")
//       + "/Profiler/WrappedData/" + jobId + "/" + localFile);
// } catch (FileNotFoundException e) {
//   // TODO Auto-generated catch block
//   e.printStackTrace();
// }
// try {
//   this.messagesBook.build().writeTo(output);
// } catch (IOException e) {
//   // TODO Auto-generated catch block
//   e.printStackTrace();
// }
// try {
//   output.close();
// } catch (IOException e) {
//   // TODO Auto-generated catch block
//   e.printStackTrace();
// }
//}
  
}
