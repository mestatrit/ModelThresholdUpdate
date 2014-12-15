package com.sharethis.adoptimization.common;

import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class GeneratingDummyVariables extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;	
	private List<String> keyCatList = null;

	public GeneratingDummyVariables(Fields fieldDeclaration, List<String> keyCatList)
    {
		super(fieldDeclaration);
		this.keyCatList = keyCatList;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		TupleEntry arguments = functionCall.getArguments();
		String keyVal = arguments.getString(0);
		double ctrVal = arguments.getDouble(1);
		Tuple group = new Tuple();
		for(int i=0; i<keyCatList.size(); i++){
			if(keyCatList.get(i).equalsIgnoreCase(keyVal)){
				group.addAll(ctrVal);
			}else{
				group.addAll(0);
			}
		}		
		functionCall.getOutputCollector().add(group);		
    }
}
