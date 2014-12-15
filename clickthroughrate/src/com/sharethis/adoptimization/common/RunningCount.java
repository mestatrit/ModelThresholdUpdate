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


public class RunningCount extends BaseOperation<NullContext> implements Buffer<NullContext> { 

	private static final long serialVersionUID = 1L;

	public RunningCount(Fields accumulatedField) { 
		super(accumulatedField); 
	} 

	@Override 
	public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) { 
		int count = 0; 
		Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator(); 

		while (iter.hasNext()) { 
			count ++; 
			bufferCall.getOutputCollector().add(new Tuple(count)); 
		} 
	} 
} 

