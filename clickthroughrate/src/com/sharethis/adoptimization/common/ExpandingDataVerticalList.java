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

public class ExpandingDataVerticalList extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(ExpandingDataVerticalList.class);
	private Fields dataFields;
	private int vFieldInd;
	private int cFieldInd;
	private int iFieldInd;
	private int pFieldInd;

	public ExpandingDataVerticalList(Fields fieldDeclaration, Fields dataFields, int vFieldInd, int cFieldInd, int iFieldInd, 
			int pFieldInd)
    {
		super(fieldDeclaration);
		this.dataFields = dataFields;
		this.vFieldInd = vFieldInd;
		this.cFieldInd = cFieldInd;
		this.iFieldInd = iFieldInd;
		this.pFieldInd = pFieldInd;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		TupleEntry arguments = functionCall.getArguments();
		LOG.info("Vaertical Field Position: " + arguments.getString(vFieldInd));
		String verticalListStr = arguments.getString(vFieldInd);
		if(verticalListStr != null && !verticalListStr.isEmpty() && !"null".equalsIgnoreCase(verticalListStr)){
			String[] verticalPairs = StringUtils.split(verticalListStr,"|");
			if(verticalPairs!=null&&verticalPairs.length>0)
				useResultSelectors(functionCall.getArguments(), functionCall.getOutputCollector(), verticalPairs);
			else{
				Tuple group = arguments.selectTuple(dataFields);
				double click = Double.parseDouble(arguments.getString(cFieldInd));
				double imp = Double.parseDouble(arguments.getString(iFieldInd));
				double cost = arguments.getDouble(pFieldInd)/1000.0;
				Tuple gp_tmp = new Tuple("null");
				group.addAll(gp_tmp);
				gp_tmp = new Tuple(1);
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
			Tuple group = arguments.selectTuple(dataFields);
			double click = Double.parseDouble(arguments.getString(cFieldInd));
			double imp = Double.parseDouble(arguments.getString(iFieldInd));
			double cost = arguments.getDouble(pFieldInd)/1000.0;
			Tuple gp_tmp = new Tuple("null");
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(1);
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
	
	private void useResultSelectors(TupleEntry input, TupleEntryCollector outputCollector, String[] verticalPairs)
    {
		if(LOG.isDebugEnabled())
			LOG.debug("Size of the Vertical Pairs: " + verticalPairs.length);
		LOG.info("Size of the Vertical Pairs: " + verticalPairs.length);
		double click = Double.parseDouble(input.getString(cFieldInd));
		double imp = Double.parseDouble(input.getString(iFieldInd));
		double cost = input.getDouble(pFieldInd)/1000.0;
		String[] keyVals = new String[verticalPairs.length];
		double[] wtVals = new double[verticalPairs.length];
		double wtSum = 0;
		for(int i=0; i<verticalPairs.length; i++){
			if(verticalPairs[i].contains(",")){
				String[] twoVals = StringUtils.split(verticalPairs[i],",");
				if(twoVals[0] != null)
					keyVals[i] = twoVals[0];	
				else
					keyVals[i] = "null";
				if(twoVals[1] != null){
					wtVals[i] = Double.parseDouble(twoVals[1]);
				}else
					wtVals[i] = 1;
				wtSum += wtVals[i];
			}else{
				keyVals[i] = verticalPairs[i];
				wtVals[i] = 1.0;
				wtSum += wtVals[i];
			}
		}
		for(int i=0; i<verticalPairs.length; i++){
			double wt = 1.0/verticalPairs.length;
			if (wtSum>=0.0001)
				wt = wtVals[i]/wtSum;
			
			Tuple group = input.selectTuple(dataFields);
			Tuple gp_tmp = new Tuple(keyVals[i]);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(wt);
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
	
	private void useResultSelectorsNew(TupleEntry input, TupleEntryCollector outputCollector, String[] verticalPairs)
    {
		if(LOG.isDebugEnabled())
			LOG.debug("Size of the Vertical Pairs: " + verticalPairs.length);
		LOG.info("Size of the Vertical Pairs: " + verticalPairs.length);
		double click = Double.parseDouble(input.getString(cFieldInd));
		double imp = Double.parseDouble(input.getString(iFieldInd));
		double cost = input.getDouble(pFieldInd)/1000.0;
		String[] keyVals = new String[verticalPairs.length];
		double[] wtVals = new double[verticalPairs.length];
		String keyVal = null;
		double wtValMax = 0;
		for(int i=0; i<verticalPairs.length; i++){
			if(verticalPairs[i].contains(",")){
				String[] twoVals = StringUtils.split(verticalPairs[i],",");
				if(twoVals[0] != null)
					keyVals[i] = twoVals[0];	
				else
					keyVals[i] = "null";
				if(twoVals[1] != null){
					wtVals[i] = Double.parseDouble(twoVals[1]);
				}else
					wtVals[i] = 0;
			}else{
				keyVals[i] = verticalPairs[i];
				wtVals[i] = 1.0;
			}
			if(wtVals[i]>=wtValMax&&!"null".equalsIgnoreCase(keyVals[i])){
				wtValMax=wtVals[i];
				keyVal=keyVals[i];
			}
		}
		Tuple group = input.selectTuple(dataFields);
		Tuple gp_tmp = new Tuple(keyVal);
		group.addAll(gp_tmp);
		gp_tmp = new Tuple(1);
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
