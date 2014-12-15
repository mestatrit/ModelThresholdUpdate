package com.sharethis.adoptimization.common;

import java.util.Random;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class RandomlyPickingKeyValue extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(RandomlyPickingKeyValue.class);
	private Fields dataFields;
	private int uFieldInd;
	private int vFieldInd;

	public RandomlyPickingKeyValue(Fields fieldDeclaration, Fields dataFields, int uFieldInd, int vFieldInd)
    {
		super(fieldDeclaration);
		this.dataFields = dataFields;
		this.uFieldInd = uFieldInd;
		this.vFieldInd = vFieldInd;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		TupleEntry arguments = functionCall.getArguments();
		LOG.info("User List Field Position: " + arguments.getString(uFieldInd));
		String userListStr = arguments.getString(uFieldInd);
		LOG.info("User List Field Position: " + arguments.getString(vFieldInd));
		String vertListStr = arguments.getString(vFieldInd);
		userSegSelectors(functionCall.getArguments(), functionCall.getOutputCollector(), userListStr, vertListStr);
    }
	
	private void userSegSelectors(TupleEntry input, TupleEntryCollector outputCollector, 
			String userListStr, String vertListStr)
    {
		Tuple group = input.selectTuple(dataFields);
		if(userListStr != null && !userListStr.isEmpty() && !"null".equalsIgnoreCase(userListStr)){
			String[] userListArr = StringUtils.split(userListStr, "|");
			String[] userKey = new String[userListArr.length];
	    	for(int i=0; i<userListArr.length; i++){
				String[] userPairs = StringUtils.split(userListArr[i], ",");
				userKey[i] = userPairs[0]; 	    		
	    	}
	    	Random ran = new Random();
	    	int randInd = ran.nextInt(userListArr.length);	
	    	Tuple gp_tmp = new Tuple(userKey[randInd]);
	    	group.addAll(gp_tmp);
		}else{
			Tuple gp_tmp = new Tuple("null");
			group.addAll(gp_tmp);			
		}
		if(vertListStr != null && !vertListStr.isEmpty() && !"null".equalsIgnoreCase(vertListStr)){		
			String[] vertListArr = StringUtils.split(vertListStr, "|");
			String[] vertKey = new String[vertListArr.length];
			int max_val_ind = -1;
			double max_val = -1;
			for(int i=0; i<vertListArr.length; i++){
				String[] vertPairs = StringUtils.split(vertListArr[i], ",");
				vertKey[i] = vertPairs[0]; 
				double vertVal = Double.parseDouble(vertPairs[1]);
				if(vertVal>max_val){
					max_val = vertVal;
					max_val_ind = i;
				}
			}	
			//Random ran = new Random();
			//int randInd = ran.nextInt(vertKey.length);	
			//Tuple gp_tmp = new Tuple(vertKey[randInd]);
			Tuple gp_tmp = new Tuple(vertKey[max_val_ind]);
			group.addAll(gp_tmp);
		}else{
			Tuple gp_tmp = new Tuple("null");
			group.addAll(gp_tmp);						
		}
		outputCollector.add(group);
    }
}
