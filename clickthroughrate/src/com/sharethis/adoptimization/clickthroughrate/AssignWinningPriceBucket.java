package com.sharethis.adoptimization.clickthroughrate;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


/** 
 */

public class AssignWinningPriceBucket extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;
	private float bkWidth;

	public AssignWinningPriceBucket(Fields fields, float bkWidth) {
		super(1, fields);
		this.bkWidth = bkWidth;
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		TupleEntry arguments = functionCall.getArguments();
		String wpStr = arguments.getString(0);
		int bkInd = 0;
		if((wpStr != null) 
				&& !("null".equalsIgnoreCase(wpStr)) 
				&& !(wpStr.isEmpty()))
			bkInd = Math.round(Float.parseFloat(wpStr)*100)/Math.round(bkWidth*100);
		Tuple result = new Tuple();
		result.add(bkInd);
		functionCall.getOutputCollector().add(result);
	}
}
