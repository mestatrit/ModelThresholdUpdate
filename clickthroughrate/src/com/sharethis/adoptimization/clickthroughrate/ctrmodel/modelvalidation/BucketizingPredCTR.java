package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


/** 
 */

public class BucketizingPredCTR extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;
	private int ctrInd = -1;
	private double[] ctrThVals;
	
	public BucketizingPredCTR(Fields fields, int ctrInd, double[] ctrThVals) {
		super(1, fields);
		this.ctrInd = ctrInd;
		this.ctrThVals = ctrThVals;
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		TupleEntry arguments = functionCall.getArguments();
		double ctrPred = 0;
		if(arguments.getString(ctrInd) != null){
			ctrPred = arguments.getDouble(ctrInd);
		}
		int id = 0;
		for(int i=1; i<ctrThVals.length; i++){
			if(ctrPred>ctrThVals[i-1]&&ctrPred<=ctrThVals[i])
				id = i;
		}
		if(ctrPred>ctrThVals[ctrThVals.length-1])
			id = ctrThVals.length;

		functionCall.getOutputCollector().add(new Tuple(id));
	}
}
