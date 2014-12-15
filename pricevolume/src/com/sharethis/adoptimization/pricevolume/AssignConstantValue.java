package com.sharethis.adoptimization.pricevolume;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;


/** 
 */

public class AssignConstantValue extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;

	private int numOfDays = -1;
	private String pDateStr = null;

	public AssignConstantValue(Fields fields, int numOfDays, String pDateStr) {
		super(1, fields);
		this.numOfDays = numOfDays;
		this.pDateStr = pDateStr;
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		Tuple result = new Tuple();
		result.add(numOfDays);
		result.add(pDateStr);
		functionCall.getOutputCollector().add(result);
	}
}
