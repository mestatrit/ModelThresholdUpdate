package com.sharethis.adoptimization.common;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


/** 
 */

public class ComputingRatio extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;
	public ComputingRatio(Fields fields) {
		super(1, fields);
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		TupleEntry arguments = functionCall.getArguments();
		double ratio = 0;
		if(arguments.getString(0) != null && arguments.getString(1) != null){
			ratio = Math.max(arguments.getDouble(0),0)/Math.max(arguments.getDouble(1),1);
			ratio = Math.round(ratio*100000)/100.0;
		}
		functionCall.getOutputCollector().add(new Tuple(ratio));
	}
}
