package org.apache.giraph.givip.profiler;

import java.io.IOException;



import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.protobuf.AbstractMessageLite;
import com.unipg.givip.common.protoutils.LatenciesProtoBook.LatenciesBook;
import com.unipg.givip.common.protoutils.LatenciesProtoBook.RecordedLatency;

/**
 * @author alessio
 *
 */
public class LatenciesWrapper extends Writer{

	private String directory = "profiler";
	private String fileName = "Latencies";
	public String jobId = null;

	private LatenciesBook.Builder latenciesBook;
	private RecordedLatency.Builder recordedLatency;
	private long currentSuperstep;
	private WorkerInfo worker;

	/**
	 * @throws
	 */
	public LatenciesWrapper(Configuration conf, WorkerInfo worker, String jobId) {
		super(conf);
		this.jobId = jobId;
		this.worker = worker;
		this.latenciesBook = LatenciesBook.newBuilder();
		this.recordedLatency = RecordedLatency.newBuilder();
		currentSuperstep = 0;
	}

	/**
	 * @param pingSource
	 * @param pingTarget
	 * @param messageSize
	 * @param isInternal
	 * @throws IOException
	 */
	public void newExchangedMessage(
			String pingTarget, long latency) throws IOException {
		this.recordedLatency.setLatencyMs(latency);
		this.recordedLatency.setPingTarget(pingTarget);
		this.recordedLatency.setPingSource(worker.getHostname());

		//Add an singleExchangedMessage between
		this.latenciesBook.addRecordedLatency(this.recordedLatency.build());
	}

	public void updateSuperstep(long superstep){
		this.currentSuperstep = superstep;
	}

	public AbstractMessageLite generateRecordToWrite(){
		return this.latenciesBook.build();
	}

	public String generateFullPath(){
		return 
				this.directory + Path.SEPARATOR  
				+ this.jobId + Path.SEPARATOR 
				+"WorkerData" + Path.SEPARATOR 
				+"LatenciesData"+ Path.SEPARATOR
				+"WorkerN-"+this.worker.getTaskId() + Path.SEPARATOR
				+this.fileName
				+"SuperstepN-" + this.currentSuperstep;
	}

	@Override
	public void clearMessages() {
		this.latenciesBook.clear();
	}

	@Override
	public int countMessages() {
		return this.latenciesBook.build().getRecordedLatencyCount();
	}

}
