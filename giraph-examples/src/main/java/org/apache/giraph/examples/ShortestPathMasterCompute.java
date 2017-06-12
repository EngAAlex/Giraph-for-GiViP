package org.apache.giraph.examples;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;

public class ShortestPathMasterCompute extends DefaultMasterCompute {

	public static final String finalAggregator = "FINAL_AGG";

	@Override
	public void compute() {
		super.compute();
		if(getSuperstep() != 0)
			if(((IntWritable)getAggregatedValue(finalAggregator)).get() == getTotalNumVertices())
				haltComputation();
	}

	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		super.initialize();
		registerAggregator(finalAggregator, IntSumAggregator.class);
	}

}
