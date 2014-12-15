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

public class MassagingKeyCTRValue extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;
	private int thresholdVal = 1000;
	private String defaultKeyVal;
	private int numKeyFields = 1;
	
	public MassagingKeyCTRValue(Fields fields, int thresholdVal, String defaultKeyVal) {
		super(1, fields);
		this.thresholdVal = thresholdVal;
		this.defaultKeyVal = defaultKeyVal;
	}

	public MassagingKeyCTRValue(Fields fields, int thresholdVal, String defaultKeyVal, int numKeyFields) {
		super(1, fields);
		this.thresholdVal = thresholdVal;
		this.defaultKeyVal = defaultKeyVal;
		this.numKeyFields = numKeyFields;
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		TupleEntry arguments = functionCall.getArguments();
		Tuple aTuple = new Tuple();
		if(arguments.getDouble(numKeyFields)<thresholdVal){
			for(int i=0; i<numKeyFields; i++){
				String keyVal = defaultKeyVal;
				aTuple.addAll(keyVal);
			}
			double impVal = arguments.getDouble("sum_imps_all");
			aTuple.addAll(impVal);
			double clkVal = arguments.getDouble("sum_clicks_all");
			aTuple.addAll(clkVal);
			double ctrVal = arguments.getDouble("ctr_all");
			aTuple.addAll(ctrVal);
		}else{
			for(int i=0; i<numKeyFields; i++){
				String keyVal = arguments.getString(i);
				aTuple.addAll(keyVal);
			}
			double impVal = arguments.getDouble("sum_imps");
			aTuple.addAll(impVal);
			double clkVal = arguments.getDouble("sum_clicks");
			aTuple.addAll(clkVal);
			double ctrVal = arguments.getDouble("ctr");
			aTuple.addAll(ctrVal);
		}
		functionCall.getOutputCollector().add(aTuple);
	}
}
