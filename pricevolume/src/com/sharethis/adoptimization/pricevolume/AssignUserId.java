package com.sharethis.adoptimization.pricevolume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


/** 
 */

public class AssignUserId extends BaseOperation implements Function
{
	private static final long serialVersionUID = 1L;
	//private static final Logger sLogger = Logger.getLogger(AssignUserId.class);
	private Map<String, List<String>> adgroupUserMap = new HashMap<String, List<String>>();
	
	public AssignUserId(Fields fields, Map<String, List<String>> adgroupUserMap2) {
		super(1, fields);
		this.adgroupUserMap = adgroupUserMap2;
	}

	public void operate_old(FlowProcess flowProcess, FunctionCall functionCall)
	{
		List<String> userIdList = null;
		String[] rawUserIds = null;
		String commonUser = null;
		String userId = null;
		
		// get the argument's TupleEntry
		TupleEntry arguments = functionCall.getArguments();
		String adgroupId = arguments.getString(0);
		String rawUserIdList = arguments.getString(1);
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
			if(commonUserId!=null&&commonUserId.size()>0){
				int numOfUsers = commonUserId.size();
				int rand_ind = ((int) Math.round(Math.random()*10000))%numOfUsers;
				userId = commonUserId.get(rand_ind);
			}
		}
		Tuple result = new Tuple();
		result.add(commonUser);
		result.add(userId);
		functionCall.getOutputCollector().add(result);
	}
	
	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		String userListStr = null;
		String commonUser = null;
		String userId = null;
		
		// get the argument's TupleEntry
		TupleEntry arguments = functionCall.getArguments();
		userListStr = arguments.getString(1);
		if(userListStr != null && !userListStr.isEmpty() && !"null".equalsIgnoreCase(userListStr)){
			if(userListStr.contains("|")){
				String[] userListPairs = StringUtils.split(userListStr, "|");
				List<String> keyVals = new ArrayList<String>();
				int cnt = 0;
				for(int i=1; i<userListPairs.length; i++){			
					if(userListPairs[i]!=null && !userListPairs[i].isEmpty() && userListPairs[i].contains(",")){
						String[] twoVals = StringUtils.split(userListPairs[i],",");
						if(twoVals[0] != null)
							keyVals.add(cnt, twoVals[0]);	
						else
							keyVals.add(cnt, "null");
						cnt++;
					}
				}
				int numOfUsers = keyVals.size();
				int rand_ind = ((int) Math.round(Math.random()*10000))%numOfUsers;
				userId = keyVals.get(rand_ind);
			}
		}
		Tuple result = new Tuple();
		result.add(userListStr);
		result.add(userId);
		functionCall.getOutputCollector().add(result);
    }
}
