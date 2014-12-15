package com.sharethis.adoptimization.common;


import java.util.Iterator;

import org.apache.hadoop.metrics.spi.NullContext;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class RunningCounts extends BaseOperation<NullContext> implements Buffer<NullContext> { 

	private static final long serialVersionUID = 1L;
	private int numVars = 1;

	public RunningCounts(Fields accumulatedField, int numVars) { 
		super(accumulatedField); 
		this.numVars = numVars;
	} 

	@Override 
	public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) { 
		double[] count = new double[numVars];
		for(int i=0; i<numVars; i++)
			count[i] = 0;
		Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator(); 

		while (iter.hasNext()) { 
			Tuple cntTuple = new Tuple();
			Tuple iterTuple = iter.next().getTuple();
			for(int i=0; i<numVars; i++){
				count[i] += iterTuple.getDouble(i); 
				count[i] = Math.round(count[i]*10000)/10000.0;				
				cntTuple.addAll(new Tuple(count[i]));
			}
//			double avg_price = iterTuple.getDouble(2)/iterTuple.getDouble(1);					
//			avg_price = Math.round(avg_price*100000)/100.0;
//			cntTuple.addAll(new Tuple(avg_price));
			bufferCall.getOutputCollector().add(cntTuple); 
		} 
	} 
} 

