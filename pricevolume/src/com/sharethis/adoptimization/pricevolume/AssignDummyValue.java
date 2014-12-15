package com.sharethis.adoptimization.pricevolume;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;


/** 
 */

public class AssignDummyValue extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;

	private int dVal=0;

	public AssignDummyValue(Fields fields, int dVal) {
		super(1, fields);
		this.dVal = dVal;
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		Tuple result = new Tuple();
		result.add(dVal);
		functionCall.getOutputCollector().add(result);
	}
}
