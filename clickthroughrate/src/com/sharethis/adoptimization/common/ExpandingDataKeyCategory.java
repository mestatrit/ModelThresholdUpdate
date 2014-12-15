package com.sharethis.adoptimization.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import org.apache.log4j.Logger;

public class ExpandingDataKeyCategory extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(ExpandingDataKeyCategory.class);
	private Fields dataFields;
	private int uFieldInd;
	private int cFieldInd;
	private int iFieldInd;
	private int pFieldInd;
	private Map<?, ?> keyCatMap = null;

	public ExpandingDataKeyCategory(Fields fieldDeclaration, Fields dataFields, int uFieldInd, int cFieldInd, int iFieldInd, 
			int pFieldInd, Map<?, ?> keyCatMap)
    {
		super(fieldDeclaration);
		this.dataFields = dataFields;
		this.uFieldInd = uFieldInd;
		this.cFieldInd = cFieldInd;
		this.iFieldInd = iFieldInd;
		this.pFieldInd = pFieldInd;
		this.keyCatMap = keyCatMap;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		String keyId = null;
		List<?> keyCatStr = null;
		TupleEntry arguments = functionCall.getArguments();
		LOG.info("key_id Position: " + arguments.getString(uFieldInd));
		if(arguments.getString(uFieldInd) != null && keyCatMap != null){
			keyId = arguments.getString(uFieldInd);
			keyCatStr = (ArrayList<?>) keyCatMap.get(keyId);
			if(keyCatStr!=null)
				useResultSelectors(functionCall.getArguments(), functionCall.getOutputCollector(), keyCatStr);
			else
				noExpanding(arguments, functionCall);
		}else{
			noExpanding(arguments, functionCall);
		}
    }
	
	private void noExpanding(TupleEntry arguments, FunctionCall functionCall){
		Tuple group = arguments.selectTuple(dataFields);
		double click = Double.parseDouble(arguments.getString(cFieldInd));
		double imp = Double.parseDouble(arguments.getString(iFieldInd));
		double cost = arguments.getDouble(pFieldInd);
		Tuple gp_tmp = new Tuple("null");
		group.addAll(gp_tmp);
		gp_tmp = new Tuple(click);
		group.addAll(gp_tmp);
		gp_tmp = new Tuple(imp);
		group.addAll(gp_tmp);
		gp_tmp = new Tuple(cost);
		group.addAll(gp_tmp);
		functionCall.getOutputCollector().add(group);					
	}
	
	private void useResultSelectors(TupleEntry input, TupleEntryCollector outputCollector, List<?> keyCatList)
    {
		double click = Double.parseDouble(input.getString(cFieldInd));
		double imp = Double.parseDouble(input.getString(iFieldInd));
		double cost = input.getDouble(pFieldInd);
		double wt = 1.0/keyCatList.size();
		for(int i=0; i<keyCatList.size(); i++){
			Tuple group = input.selectTuple(dataFields);
			Tuple gp_tmp = new Tuple(keyCatList.get(i));
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(click*wt);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(imp*wt);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(cost*wt);
			group.addAll(gp_tmp);
			outputCollector.add(group);
		}
    }
}
