package com.sharethis.adoptimization.common;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;


/** 
 */

public class AssigningOneConstantId extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;
	private int id = 0;
	
	public AssigningOneConstantId(Fields fields, int id) {
		super(1, fields);
		this.id = id;
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		functionCall.getOutputCollector().add(new Tuple(id));
	}
}
