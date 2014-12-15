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

public class ComputingCTRValue extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;
	public ComputingCTRValue(Fields fields) {
		super(1, fields);
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		TupleEntry arguments = functionCall.getArguments();
		double ctr = 0;
		double sum_imps = 1;
		double sum_clicks = 0;
		if(arguments.getString(0) != null && arguments.getString(1) != null){
			sum_imps = Math.round(arguments.getDouble(0)*100000)/100000.0;
			sum_clicks = Math.round(arguments.getDouble(1)*100000)/100000.0;
			ctr = Math.max(sum_clicks,0)/Math.max(sum_imps,0.0001);
			ctr = Math.round(ctr*1000000)/1000000.0;
		}
		double cost = 0;

		if(arguments.getString(2) != null){
			cost = arguments.getDouble(2);
		}
		cost = Math.round(cost*1000000)/1000000.0;
		double e_cpm = Math.round(cost/sum_imps*10000000)/10000.0;

		Tuple result = new Tuple();
		result.add(sum_imps);
		result.add(sum_clicks);
		result.add(cost);
		result.add(ctr);
		result.add(e_cpm);
		if (sum_clicks>0.001){
			double e_cpc = Math.round(cost/sum_clicks*1000000)/1000000.0;		
			result.add(e_cpc);
		}else{
			result.add("\\N");
		}
		functionCall.getOutputCollector().add(result);
	}
}
