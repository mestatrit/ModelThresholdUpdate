package com.sharethis.adoptimization.inventory;

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

public class ExpandingInventoryVerticalList extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(ExpandingInventoryVerticalList.class);
	private Fields dataFields;
	private int vFieldInd;
	private int cFieldInd;

	public ExpandingInventoryVerticalList(Fields fieldDeclaration, Fields dataFields, int vFieldInd, int cFieldInd)
    {
		super(fieldDeclaration);
		this.dataFields = dataFields;
		this.vFieldInd = vFieldInd;
		this.cFieldInd = cFieldInd;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		TupleEntry arguments = functionCall.getArguments();
		LOG.info("Vaertical Field Position: " + arguments.getString(vFieldInd));
		String verticalListStr = arguments.getString(vFieldInd);
		if(verticalListStr != null && !verticalListStr.isEmpty() && !"null".equalsIgnoreCase(verticalListStr)){
			String[] verticalPairs = StringUtils.split(verticalListStr,"|");
			useResultSelectors(functionCall.getArguments(), functionCall.getOutputCollector(), verticalPairs);
		}else{
			Tuple group = arguments.selectTuple(dataFields);
			double req_cnt = Double.parseDouble(arguments.getString(cFieldInd));
			Tuple gp_tmp = new Tuple("null");
			group.addAll(gp_tmp);
//			gp_tmp = new Tuple(1);
//			group.addAll(gp_tmp);
			gp_tmp = new Tuple(req_cnt);
			group.addAll(gp_tmp);
			functionCall.getOutputCollector().add(group);			
		}
    }
	
	private void useResultSelectors(TupleEntry input, TupleEntryCollector outputCollector, String[] verticalPairs)
    {
		if(LOG.isDebugEnabled())
			LOG.debug("Size of the Vertical Pairs: " + verticalPairs.length);
		LOG.info("Size of the Vertical Pairs: " + verticalPairs.length);
		double req_cnt = Double.parseDouble(input.getString(cFieldInd));
		for(int i=0; i<verticalPairs.length; i++){
			Tuple group = input.selectTuple(dataFields);
			LOG.info("VerticalPairs: " + verticalPairs[i]);
			if(verticalPairs[i]!=null){
				String[] twoVals = StringUtils.split(verticalPairs[i],",");
				LOG.info("VerticalPairs: " + twoVals.toString());
				if(twoVals!=null&&twoVals.length>1){					
					Tuple gp_tmp = new Tuple(twoVals[0]);
					group.addAll(gp_tmp);
//					gp_tmp = new Tuple(twoVals[1]);
//					group.addAll(gp_tmp);
					double twoVal_tmp = Double.parseDouble(twoVals[1]);
					gp_tmp = new Tuple(req_cnt*twoVal_tmp);
					group.addAll(gp_tmp);
				}else{
					Tuple gp_tmp = new Tuple("null");
					group.addAll(gp_tmp);
//					gp_tmp = new Tuple(1);
//					group.addAll(gp_tmp);
					gp_tmp = new Tuple(req_cnt);
					group.addAll(gp_tmp);
				}
			}
			outputCollector.add(group);
		}
    }
}
