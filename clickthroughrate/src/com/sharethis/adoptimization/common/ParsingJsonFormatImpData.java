package com.sharethis.adoptimization.common;

import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.google.gson.Gson;
import com.sharethis.adoptimization.campaigndata.HourlyDataImpressionObject;

public class ParsingJsonFormatImpData extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;	
	private String[] impNames = null;

	public ParsingJsonFormatImpData(Fields fieldDeclaration, String[] impNames)
    {
		super(fieldDeclaration);
		this.impNames = impNames;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		TupleEntry arguments = functionCall.getArguments();
		String jsonRow = arguments.getString(0);
		Gson gson = new Gson();
		if(jsonRow!=null && !("null".equalsIgnoreCase(jsonRow))&&!(jsonRow.isEmpty())){
			HourlyDataImpressionObject obj = new HourlyDataImpressionObject();
			obj = gson.fromJson(jsonRow, HourlyDataImpressionObject.class);
			Map dMap = obj.toMap();
			Tuple group = new Tuple();
			for(int i=0; i<impNames.length;i++){
				Tuple gp = new Tuple(dMap.get(impNames[i]));
				group.addAll(gp);
			}
			functionCall.getOutputCollector().add(group);		
		}else{
			System.out.println("The row is empty!");
		}
    }
}
