package com.sharethis.adoptimization.campaigndata;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


/** 
 */

public class AssignClickImpInt extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;

	public AssignClickImpInt(Fields fields) {
		super(1, fields);
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		TupleEntry arguments = functionCall.getArguments();
		int clickVal = 0;
		int impVal = 1;
		if((arguments.getString(0) != null) 
				&& !("null".equalsIgnoreCase(arguments.getString(0))) 
				&& !(arguments.getString(0).isEmpty())
				&& (arguments.getString(0)).equalsIgnoreCase(arguments.getString(1)))
			clickVal = 1;
		Tuple result = new Tuple();
		result.add(clickVal);
		result.add(impVal);
		functionCall.getOutputCollector().add(result);
	}
}
