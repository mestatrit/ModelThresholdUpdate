package com.sharethis.adoptimization.common;

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

public class ExpandingDataUserList extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(ExpandingDataUserList.class);
	private Fields dataFields;
	private int uFieldInd;
	private int cFieldInd;
	private int iFieldInd;
	private int pFieldInd;

	public ExpandingDataUserList(Fields fieldDeclaration, Fields dataFields, int uFieldInd, int cFieldInd, int iFieldInd, 
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
			useResultSelectors(functionCall.getArguments(), functionCall.getOutputCollector(), userListStr);
		}else{
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
