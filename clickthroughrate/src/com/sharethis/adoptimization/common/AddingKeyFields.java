package com.sharethis.adoptimization.common;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AddingKeyFields extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Fields keyFields;

	public AddingKeyFields(Fields fieldDeclaration, Fields keyFields)
    {
		super(fieldDeclaration);
		this.keyFields = keyFields;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		TupleEntry arguments = functionCall.getArguments();
		Tuple group = new Tuple();
		String overallKey = arguments.getString(0);
		group.addAll(new Tuple(overallKey));
		int keyFieldsLen = keyFields.size();
		for(int i=0; i<keyFieldsLen; i++)
			group.addAll(new Tuple("default_key"));
		for(int i=1; i<arguments.size(); i++)
			group.addAll(new Tuple(arguments.getString(i)));
		functionCall.getOutputCollector().add(group);					
    }
}
