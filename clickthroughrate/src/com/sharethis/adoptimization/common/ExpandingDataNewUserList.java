package com.sharethis.adoptimization.common;

import java.util.ArrayList;
import java.util.List;

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

public class ExpandingDataNewUserList extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(ExpandingDataNewUserList.class);
	private Fields dataFields;
	private int uFieldInd;
	private int cFieldInd;
	private int iFieldInd;
	private int pFieldInd;

	public ExpandingDataNewUserList(Fields fieldDeclaration, Fields dataFields, int uFieldInd, int cFieldInd, int iFieldInd, 
			int pFieldInd)
    {
		super(fieldDeclaration);
		this.dataFields = dataFields;
		this.uFieldInd = uFieldInd;
		this.cFieldInd = cFieldInd;
		this.iFieldInd = iFieldInd;
		this.pFieldInd = pFieldInd;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		String userListStr = null;
		TupleEntry arguments = functionCall.getArguments();
		LOG.info("User List Field Position: " + arguments.getString(uFieldInd));
		userListStr = arguments.getString(uFieldInd);
		if(userListStr != null && !userListStr.isEmpty() && !"null".equalsIgnoreCase(userListStr)){
			if(userListStr.contains("|")){
				String[] userListPairs = StringUtils.split(userListStr, "|");
				if(userListPairs != null && userListPairs.length > 0){
					useResultSelectorsNew(functionCall.getArguments(), functionCall.getOutputCollector(), userListPairs);
				}else{
					Tuple group = arguments.selectTuple(dataFields);
					double click = Double.parseDouble(arguments.getString(cFieldInd));
					double imp = Double.parseDouble(arguments.getString(iFieldInd));
					double cost = arguments.getDouble(pFieldInd);
					Tuple gp_tmp = new Tuple("null");
					group.addAll(gp_tmp);
					gp_tmp = new Tuple(-1);
					group.addAll(gp_tmp);
					gp_tmp = new Tuple(click);
					group.addAll(gp_tmp);
					gp_tmp = new Tuple(imp);
					group.addAll(gp_tmp);
					gp_tmp = new Tuple(cost);
					group.addAll(gp_tmp);
					functionCall.getOutputCollector().add(group);							
				}
			}else{
				useResultSelectors(functionCall.getArguments(), functionCall.getOutputCollector(), userListStr);
			}
		}else{
			Tuple group = arguments.selectTuple(dataFields);
			double click = Double.parseDouble(arguments.getString(cFieldInd));
			double imp = Double.parseDouble(arguments.getString(iFieldInd));
			double cost = arguments.getDouble(pFieldInd);
			Tuple gp_tmp = new Tuple("null");
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(-1);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(click);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(imp);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(cost);
			group.addAll(gp_tmp);
			functionCall.getOutputCollector().add(group);			
		}
    }
	
	private void useResultSelectorsNew(TupleEntry input, TupleEntryCollector outputCollector, String[] userListPairs)
    {
		if(LOG.isDebugEnabled())
			LOG.debug("Size of the User List Pairs: " + userListPairs.length);
		double click = Double.parseDouble(input.getString(cFieldInd));
		double imp = Double.parseDouble(input.getString(iFieldInd));
		double cost = input.getDouble(pFieldInd);
//		double cost = input.getDouble(pFieldInd)/1000.0;
		List<String> keyVals = new ArrayList<String>();
		List<Long> ageVals = new ArrayList<Long>();
		int cnt = 0;
		for(int i=0; i<userListPairs.length; i++){			
			if(userListPairs[i]!=null && !userListPairs[i].isEmpty() && userListPairs[i].contains(",")){
				String[] twoVals = StringUtils.split(userListPairs[i],",");
				if(twoVals[0] != null)
					keyVals.add(cnt, twoVals[0]);	
				else
					keyVals.add(cnt, "null");
				if(twoVals[1] != null && !twoVals[1].isEmpty() && !"null".equalsIgnoreCase(twoVals[1])){
					ageVals.add(cnt, Long.parseLong(twoVals[1]));
				}else{
					ageVals.add(cnt, -1L);
				}
				cnt++;
			}
		}
		double wt = 1.0;
		if(cnt>=1){ 
			wt = 1.0/cnt;
			for(int i=0; i<cnt; i++){
				Tuple group = input.selectTuple(dataFields);
				Tuple gp_tmp = new Tuple(keyVals.get(i));
				group.addAll(gp_tmp);
				gp_tmp = new Tuple(ageVals.get(i));
				group.addAll(gp_tmp);
				gp_tmp = new Tuple(click*wt);
				group.addAll(gp_tmp);
				gp_tmp = new Tuple(imp*wt);
				group.addAll(gp_tmp);
				gp_tmp = new Tuple(cost*wt);
				group.addAll(gp_tmp);
				outputCollector.add(group);
			}
		}else{
			Tuple group = input.selectTuple(dataFields);
			Tuple gp_tmp = new Tuple("null");
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(-1);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(click);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(imp);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(cost);
			group.addAll(gp_tmp);
			outputCollector.add(group);				
		}
    }	
	
	private void useResultSelectors(TupleEntry input, TupleEntryCollector outputCollector, String userListStr)
    {
		double click = Double.parseDouble(input.getString(cFieldInd));
		double imp = Double.parseDouble(input.getString(iFieldInd));
		double cost = input.getDouble(pFieldInd);
		String[] userListArr = StringUtils.split(userListStr, ",");
		double wt = 1.0/userListArr.length;
		for(int i=0; i<userListArr.length; i++){
			Tuple group = input.selectTuple(dataFields);
			Tuple gp_tmp = new Tuple(userListArr[i]);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(-1);
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
