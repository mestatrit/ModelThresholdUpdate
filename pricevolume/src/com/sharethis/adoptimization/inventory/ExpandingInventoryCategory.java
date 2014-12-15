package com.sharethis.adoptimization.inventory;

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

public class ExpandingInventoryCategory extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(ExpandingInventoryCategory.class);
	private Fields dataFields;
	private int uFieldInd;
	private int cFieldInd;
	private int bpFieldInd;
	private int mbpFieldInd;
	private Map<?, ?> keyCatMap = null;

	public ExpandingInventoryCategory(Fields fieldDeclaration, Fields dataFields, int uFieldInd, 
			int cFieldInd, int bpFieldInd, int mbpFieldInd, Map<?, ?> keyCatMap)
    {
		super(fieldDeclaration);
		this.dataFields = dataFields;
		this.uFieldInd = uFieldInd;
		this.cFieldInd = cFieldInd;
		this.bpFieldInd = bpFieldInd;
		this.mbpFieldInd = mbpFieldInd;
		this.keyCatMap = keyCatMap;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		List<?> keyCatStr = null;
		TupleEntry arguments = functionCall.getArguments();
		String keyId = arguments.getString(uFieldInd);
		LOG.info("key_id Position: " + keyId);
		double req_cnt = Double.parseDouble(arguments.getString(cFieldInd));
		double bp = arguments.getDouble(bpFieldInd);
		double mbp = arguments.getDouble(mbpFieldInd);
		if(keyId != null && keyCatMap != null){
			keyCatStr = (ArrayList<?>) keyCatMap.get(keyId);
			if(keyCatStr!=null)
				useResultSelectors(functionCall.getArguments(), functionCall.getOutputCollector(), keyCatStr, req_cnt, bp, mbp);
			else
				noExpanding(arguments, functionCall, req_cnt, bp, mbp);
		}else{
			noExpanding(arguments, functionCall, req_cnt, bp, mbp);
		}
    }
	
	private void noExpanding(TupleEntry arguments, FunctionCall functionCall, double req_cnt, double bp, double mbp){
		Tuple group = arguments.selectTuple(dataFields);
		Tuple gp_tmp = new Tuple("null");
		group.addAll(gp_tmp);
		gp_tmp = new Tuple(req_cnt);
		group.addAll(gp_tmp);
		gp_tmp = new Tuple(bp);
		group.addAll(gp_tmp);
		gp_tmp = new Tuple(mbp);
		group.addAll(gp_tmp);
		functionCall.getOutputCollector().add(group);					
	}
	
	private void useResultSelectors(TupleEntry input, TupleEntryCollector outputCollector, List<?> keyCatList, 
			double req_cnt, double bp, double mbp)
    {
		double wt = 1.0/keyCatList.size();
		for(int i=0; i<keyCatList.size(); i++){
			Tuple group = input.selectTuple(dataFields);
			Tuple gp_tmp = new Tuple(keyCatList.get(i));
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(req_cnt*wt);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(bp*wt);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(mbp*wt);
			group.addAll(gp_tmp);
			outputCollector.add(group);
		}
    }
}
