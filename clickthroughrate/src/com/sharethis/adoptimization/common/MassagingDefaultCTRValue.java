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

public class MassagingDefaultCTRValue extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;
	private String defaultKeyVal;
	private int numKeyFields = 1;
	private String name = null;

	public MassagingDefaultCTRValue(Fields fields, String defaultKeyVal, int numKeyFields, String name) {
		super(1, fields);
		this.defaultKeyVal = defaultKeyVal;
		this.numKeyFields = numKeyFields;
		this.name= name;
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		TupleEntry arguments = functionCall.getArguments();
		Tuple aTuple = new Tuple();
		for(int i=0; i<numKeyFields; i++){
			String keyVal = defaultKeyVal;
			aTuple.addAll(keyVal);
		}
		double impVal = arguments.getDouble("sum_imps");
		aTuple.addAll(impVal);
		double clkVal = arguments.getDouble("sum_clicks");
		double ctrVal = arguments.getDouble("ctr");
		if(name.contains("mappid")||name.contains("mcrid")||name.contains("mdsidt")
				||name.contains("mdsmd")||name.contains("mdsmk")||name.contains("mdst")){
			aTuple.addAll(clkVal);
			aTuple.addAll(ctrVal);
		}else{
			aTuple.addAll(impVal*0.00045);
			aTuple.addAll(0.00045);
		}
		functionCall.getOutputCollector().add(aTuple);
	}
}
