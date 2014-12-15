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

public class ConstructingCTRKeys extends BaseOperation implements Function
{
	private static final long serialVersionUID = 1L;
	private String type = null;

	public ConstructingCTRKeys(Fields fields, String type) {
		super(1, fields);
		this.type = type;
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		TupleEntry arguments = functionCall.getArguments();
		String keyStr = "null";
		int argSize = arguments.size();
		if(argSize>=1){
			keyStr = arguments.getString(0);
			for(int i=1; i<argSize; i++){
				keyStr = keyStr + "|" + arguments.getString(i);
			}
		}
		Tuple result = new Tuple();
		result.add(type);
		result.add(keyStr);
		functionCall.getOutputCollector().add(result);
	}
	
/*
	public ConstructingCTRKeys(Fields fields, String type, boolean pt_flag) {
		super(1, fields);
		this.type = type;
		this.pt_flag = pt_flag;
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		TupleEntry arguments = functionCall.getArguments();
		String keyStr = "null";
		int argSize = arguments.size();
		Tuple result = new Tuple();
		if(pt_flag){
			String platform = "null";
			if(argSize>=2){
				platform = arguments.getString(0);
				keyStr = arguments.getString(1);
				for(int i=2; i<argSize; i++){
					keyStr = keyStr + "|" + arguments.getString(i);
				}
			}
			result.add(platform);
			result.add(type);
			result.add(keyStr);
		}else{
			if(argSize>=1){
				keyStr = arguments.getString(0);
				for(int i=1; i<argSize; i++){
					keyStr = keyStr + "|" + arguments.getString(i);
				}
			}
			result.add(type);
			result.add(keyStr);			
		}
		functionCall.getOutputCollector().add(result);
	}
	*/
}
