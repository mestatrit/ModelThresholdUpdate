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

public class ConvertingSecondToHour extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;
	public ConvertingSecondToHour(Fields fields) {
		super(1, fields);
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		Tuple group = new Tuple();
		TupleEntry arguments = functionCall.getArguments();
		long hour_0 = -1;
		if(arguments.getString(0) != null && !"null".equalsIgnoreCase(arguments.getString(0)) 
				&& !arguments.getString(0).isEmpty() && !"-1".equalsIgnoreCase(arguments.getString(0))){
			hour_0 = Long.parseLong(arguments.getString(0))/3600;
		}
		Tuple gp_tmp = new Tuple(hour_0);
		group.addAll(gp_tmp);
		long hour_1 = -1;
		if(arguments.getString(1) != null && !"null".equalsIgnoreCase(arguments.getString(1)) 
				&& !arguments.getString(1).isEmpty()  && !"-1".equalsIgnoreCase(arguments.getString(1))){
			hour_1 = Long.parseLong(arguments.getString(1))/3600;
		}
		gp_tmp = new Tuple(hour_1);
		group.addAll(gp_tmp);
		functionCall.getOutputCollector().add(group);	
	}
}
