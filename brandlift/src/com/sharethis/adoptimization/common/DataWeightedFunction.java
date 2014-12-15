package com.sharethis.adoptimization.common;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DataWeightedFunction extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Fields dataFields;
	private int numOfKeyFields = 0;
	private int numOfDataFields = 0;
	private double wt = 1;

	public DataWeightedFunction(Fields fieldDeclaration, Fields dataFields, int numOfKeyFields, int numOfDataFields, double wt)
    {
		super(fieldDeclaration);
		this.dataFields = dataFields;
		this.wt = wt;
		this.numOfKeyFields = numOfKeyFields;
		this.numOfDataFields = numOfDataFields;
    }	

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		TupleEntry arguments = functionCall.getArguments();
		Tuple group = arguments.selectTuple(dataFields);
		for(int i=numOfKeyFields; i<numOfKeyFields+numOfDataFields; i++){
			double val = 0;
			String valStr = arguments.getString(i);
			if(!"\\N".equalsIgnoreCase(valStr)&&valStr!=null&&!"null".equalsIgnoreCase(valStr)){
				val = wt*Double.parseDouble(arguments.getString(i));
				group.set(i, val);
			}else{
				group.set(i, valStr);
			}
		}
		functionCall.getOutputCollector().add(group);
    }
}
