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
import com.sharethis.adoptimization.campaigndata.HourlyDataClickObject;

public class ParsingJsonFormatClickData extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;	
	private String[] clkNames = null;

	public ParsingJsonFormatClickData(Fields fieldDeclaration, String[] clkNames)
    {
		super(fieldDeclaration);
		this.clkNames = clkNames;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		TupleEntry arguments = functionCall.getArguments();
		String jsonRow = arguments.getString(0);
		Gson gson = new Gson();
		if(jsonRow!=null && !("null".equalsIgnoreCase(jsonRow))&&!(jsonRow.isEmpty())){
			HourlyDataClickObject obj = new HourlyDataClickObject();
			obj = gson.fromJson(jsonRow, HourlyDataClickObject.class);
			Map dMap = obj.toMap();
			Tuple group = new Tuple();
			for(int i=0; i<clkNames.length; i++){
				Tuple gp = new Tuple(dMap.get(clkNames[i]));
				group.addAll(gp);
			}
			functionCall.getOutputCollector().add(group);		
		}else{
			System.out.println("The row is empty!");
		}
    }
}
