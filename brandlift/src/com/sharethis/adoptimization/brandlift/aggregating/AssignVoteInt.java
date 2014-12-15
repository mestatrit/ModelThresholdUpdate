package com.sharethis.adoptimization.brandlift.aggregating;

import com.sharethis.adoptimization.common.BLConstants;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


/** 
 */

public class AssignVoteInt extends BaseOperation implements Function
{

	private static final long serialVersionUID = 1L;

	public AssignVoteInt(Fields fields) {
		super(1, fields);
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		TupleEntry arguments = functionCall.getArguments();
		int blOptionVal = -1;
		int blPositiveVal = 0;
		int blVoteVal = 0;
		if((arguments.getString(0) != null) 
				&& !("null".equalsIgnoreCase(arguments.getString(0))) 
				&& !(arguments.getString(0).isEmpty())){
			blVoteVal = 1;
			for(int i=0; i<BLConstants.voteOptions.length; i++){
				if(BLConstants.voteOptions[i].equalsIgnoreCase(arguments.getString(1))){
					blOptionVal = i;
					if(i>0) blPositiveVal = 1;
					break;
				}
			}
		}
		Tuple result = new Tuple();
		result.add(blOptionVal);
		result.add(blPositiveVal);
		result.add(blVoteVal);
		functionCall.getOutputCollector().add(result);
	}
}
