package com.sharethis.adoptimization.inventory;

import java.util.ArrayList;
import java.util.HashMap;
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

import org.apache.commons.lang.StringUtils;

public class ExpandingInventoryUserList extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Fields dataFields;
	private int aFieldInd;
	private int uFieldInd;
	private int bpFieldInd;
	private int mbpFieldInd;
	private Map<String, List<String>> adgroupUserMap = new HashMap<String, List<String>>();
	private double multiplier = 1;

	public ExpandingInventoryUserList(Fields fieldDeclaration, Fields dataFields, int aFieldInd, 
			int uFieldInd, int bpFieldInd, int mbpFieldInd, Map<String, List<String>> adgroupUserMap, double multiplier)
    {
		super(fieldDeclaration);
		this.dataFields = dataFields;
		this.aFieldInd = aFieldInd;
		this.uFieldInd = uFieldInd;
		this.bpFieldInd = bpFieldInd;
		this.mbpFieldInd = mbpFieldInd;
		this.adgroupUserMap = adgroupUserMap;
		this.multiplier = multiplier;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		List<String> userIdList = null;
		String[] rawUserIds = null;
		String commonUser = null;
		
		// get the argument's TupleEntry
		TupleEntry arguments = functionCall.getArguments();
		String adgroupId = arguments.getString(aFieldInd);
		String rawUserIdList = arguments.getString(uFieldInd);
		double bp = arguments.getDouble(bpFieldInd)/multiplier;
		double mbp = arguments.getDouble(mbpFieldInd)/multiplier;
		if(rawUserIdList!=null&&adgroupUserMap!=null&&rawUserIdList.length()>0&&adgroupUserMap.size()>0){
			rawUserIds = StringUtils.split(rawUserIdList,",");		
			userIdList = (List<String>) adgroupUserMap.get(adgroupId);
			List<String> commonUserId = new ArrayList<String>();
			int i_cnt = 0;
			if(userIdList!=null&&userIdList.size()>0){
				for(int i_r=0; i_r<rawUserIds.length; i_r++){
					for(int i=0; i<userIdList.size(); i++){
						String userIdStr = (String) userIdList.get(i);
						if(rawUserIds[i_r].equalsIgnoreCase(userIdStr)&&!commonUserId.contains(userIdStr)){
							commonUserId.add(i_cnt, userIdStr);
							if(commonUser==null)
								commonUser = userIdStr;
							else
								commonUser = commonUser + "," + userIdStr;
						}
					}
				}
			}
			if(commonUser != null && !commonUser.isEmpty() && !"null".equalsIgnoreCase(commonUser)){
				useResultSelectors(functionCall.getArguments(), functionCall.getOutputCollector(), commonUser, bp, mbp);
			}else{
				Tuple group = arguments.selectTuple(dataFields);
				double req_cnt = 1;
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
		}else{
			Tuple group = arguments.selectTuple(dataFields);
			double req_cnt = 1;
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
	}
	
	private void useResultSelectors(TupleEntry input, TupleEntryCollector outputCollector, String userListStr, double bp, double mbp)
    {
		String[] userListArr = StringUtils.split(userListStr, ",");
		double wt = 1.0/userListArr.length;
		for(int i=0; i<userListArr.length; i++){
			Tuple group = input.selectTuple(dataFields);
			Tuple gp_tmp = new Tuple(userListArr[i]);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(wt);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(bp*wt);
			group.addAll(gp_tmp);
			gp_tmp = new Tuple(mbp*wt);
			group.addAll(gp_tmp);
			outputCollector.add(group);
		}
    }
}
