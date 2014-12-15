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

public class YMassagingCTRValue extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;
	private int thresholdVal = 1000;
	private String impName = null;
	private String ctrName = null;
	private String ctrAllName = null;
	
	public YMassagingCTRValue(Fields fields, int thresholdVal, String impName, 
			String ctrName, String ctrAllName) {
		super(1, fields);
		this.thresholdVal = thresholdVal;
		this.impName = impName;
		this.ctrName = ctrName;
		this.ctrAllName = ctrAllName;
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		TupleEntry arguments = functionCall.getArguments();
		Tuple aTuple = new Tuple();
		if(arguments.getDouble(impName)<thresholdVal){
			double ctrVal = arguments.getDouble(ctrAllName);
			aTuple.addAll(ctrVal);
		}else{
			double ctrVal = arguments.getDouble(ctrName);
			aTuple.addAll(ctrVal);
		}
		functionCall.getOutputCollector().add(aTuple);
	}
}
