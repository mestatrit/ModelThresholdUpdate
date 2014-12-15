package com.sharethis.adoptimization.pricevolume;

import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


/** 
 */

public class AdjustingWinningPrice extends BaseOperation implements Function
{
	private static final long serialVersionUID = 1L;
	private static final Logger sLogger = Logger.getLogger(AdjustingWinningPrice.class);
	private double multiplier = 1.0;
	private double bucketWidth = 0.1;
	
	public AdjustingWinningPrice(Fields fields, double multiplier, double bucketWidth) {
		super(1, fields);
		this.multiplier = multiplier;
		this.bucketWidth = bucketWidth;
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		// get the argument's TupleEntry
		TupleEntry arguments = functionCall.getArguments();

		String wpStr = arguments.getString(0);
		double wp = 0.0;
		long bkInd = 0;
		try{
			wp = Double.parseDouble(wpStr);
			bkInd = Math.round(wp/multiplier*100)/Math.round(bucketWidth*100);
		}catch(Exception ex){
			sLogger.info("Thw winning price is not a number!");
		}
		Tuple result = new Tuple();
		result.add(wp/multiplier);
		result.add(bkInd);
		functionCall.getOutputCollector().add(result);
	}
}
