package com.sharethis.adoptimization.inventory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


/** 
 */

public class ComputingWeightedAverage extends BaseOperation implements Function
{
	private static final long serialVersionUID = 1L;
	
	public ComputingWeightedAverage(Fields fields) {
		super(1, fields);
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		// get the argument's TupleEntry
		TupleEntry arguments = functionCall.getArguments();

		String wpStr = arguments.getString(0);
		double wp = Double.parseDouble(wpStr);
		String wpStr1 = arguments.getString(1);
		double wp1 = Double.parseDouble(wpStr1);
		String sumStr = arguments.getString(2);
		double sum = Double.parseDouble(sumStr);
		wp = Math.round(wp/sum*100)/100.0;
		wp1 = Math.round(wp1/sum*100)/100.0;
		Tuple result = new Tuple();
		result.add(wp);
		result.add(wp1);
		functionCall.getOutputCollector().add(result);
	}
}
