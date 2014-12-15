package com.sharethis.adoptimization.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import org.apache.log4j.Logger;

public class RandomlyPickingKeyCategory extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(RandomlyPickingKeyCategory.class);
	private Fields dataFields;
	private int setFieldInd;
	private int crtvFieldInd;
	private Map<String, List<String>> setCatMap;
	private Map<String, List<String>> crtvCatMap;

	public RandomlyPickingKeyCategory(Fields fieldDeclaration, Fields dataFields, int setFieldInd, 
			int crtvFieldInd, Map<String, List<String>> setCatMap, Map<String, List<String>> crtvCatMap)
    {
		super(fieldDeclaration);
		this.dataFields = dataFields;
		this.setFieldInd = setFieldInd;
		this.crtvFieldInd = crtvFieldInd;
		this.setCatMap = setCatMap;
		this.crtvCatMap = crtvCatMap;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		String setCat = null;
		List<?> setCatStr = null;
		String crtvCat = null;
		List<?> crtvCatStr = null;
		TupleEntry arguments = functionCall.getArguments();
		LOG.info("key_id Position: " + arguments.getString(setFieldInd));
		if((arguments.getString(setFieldInd) != null && setCatMap != null)
				||(arguments.getString(crtvFieldInd) != null && crtvCatMap != null)){
			setCat = arguments.getString(setFieldInd);
			setCatStr = (ArrayList<?>) setCatMap.get(setCat);
			crtvCat = arguments.getString(crtvFieldInd);
			crtvCatStr = (ArrayList<?>) crtvCatMap.get(crtvCat);
			catSelectors(functionCall.getArguments(), functionCall.getOutputCollector(), setCatStr, crtvCatStr);
		}else{
			noExpanding(arguments, functionCall);
		}
    }

	private void noExpanding(TupleEntry arguments, FunctionCall functionCall){
		Tuple group = arguments.selectTuple(dataFields);
		Tuple gp_tmp = new Tuple("null");
		group.addAll(gp_tmp);
		gp_tmp = new Tuple("null");
		group.addAll(gp_tmp);
		functionCall.getOutputCollector().add(group);					
	}
		
	private void catSelectors(TupleEntry input, TupleEntryCollector outputCollector, 
			 List<?> setCatList, List<?> crtvCatList)
    {
		Tuple group = input.selectTuple(dataFields);
		if(setCatList!=null && !setCatList.isEmpty() && setCatList.size()!=0){
			Random ran = new Random();
			int randInd = ran.nextInt(setCatList.size());	
			Tuple gp_tmp = new Tuple(setCatList.get(randInd));
			group.addAll(gp_tmp);
		}else{
			Tuple gp_tmp = new Tuple("null");
			group.addAll(gp_tmp);			
		}
		if(crtvCatList!=null && !crtvCatList.isEmpty() && crtvCatList.size()!=0){
			Random ran = new Random();
			int randInd = ran.nextInt(crtvCatList.size());	
			Tuple gp_tmp = new Tuple(crtvCatList.get(randInd));
			group.addAll(gp_tmp);
		}else{
			Tuple gp_tmp = new Tuple("null");
			group.addAll(gp_tmp);			
		}
		outputCollector.add(group);
    }
}
