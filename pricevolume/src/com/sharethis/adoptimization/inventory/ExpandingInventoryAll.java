package com.sharethis.adoptimization.inventory;

import java.util.ArrayList;
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

public class ExpandingInventoryAll extends BaseOperation implements Function
{
	private static final long serialVersionUID = 1L;
	private double multiplier = 1.0;
	private Map<?, ?> crtvCatMap = null;
	private Map<?, ?> setCatMap = null;
	private Map<?, ?> userMap = null;
	
	public ExpandingInventoryAll(Fields fields, double multiplier, Map crtvCatMap, Map setCatMap, Map userMap) {
		super(1, fields);
		this.multiplier = multiplier;
		this.crtvCatMap = crtvCatMap;
		this.setCatMap = setCatMap;
		this.userMap = userMap;
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		// get the argument's TupleEntry
		TupleEntry arguments = functionCall.getArguments();

		String wpStr = arguments.getString(0);
		double wp = 0;
		if(wpStr!=null&&!wpStr.isEmpty()&&!"null".equalsIgnoreCase(wpStr))
			wp = Double.parseDouble(wpStr);
		String wpStr1 = arguments.getString(1);
		double wp1 = 0;
		if(wpStr1!=null&&!wpStr1.isEmpty()&&!"null".equalsIgnoreCase(wpStr1))
			wp1 = Double.parseDouble(wpStr1);
		String crtvId = arguments.getString(2);
		String crtv_cat_id = null;
		if(crtvCatMap!=null&&crtvId!=null&&!crtvCatMap.isEmpty()&&!crtvId.isEmpty()){
			List<String> crtvCatList = (List) crtvCatMap.get(crtvId);
			crtv_cat_id = assignId(crtvCatList);
		}
		
		String setId = arguments.getString(3);
		String set_cat_id = null;
		if(setCatMap!=null&&setId!=null&&!setCatMap.isEmpty()&&!setId.isEmpty()){
			List<String> setCatList = (List) setCatMap.get(setId);
			set_cat_id = assignId(setCatList);
		}
		String adgroupId = arguments.getString(4);
		String userList = arguments.getString(5);
		String user_seg_id = null;
		if(userMap!=null&&userList!=null&&adgroupId!=null&&!userMap.isEmpty()&&!userList.isEmpty()&&!adgroupId.isEmpty())		
			user_seg_id = assignUserId(adgroupId, userList, userMap);
		Tuple result = new Tuple();
		result.add(1);
		result.add(wp/multiplier);
		result.add(wp1/multiplier);
		result.add(crtv_cat_id);
		result.add(set_cat_id);
		result.add(user_seg_id);
		result.add(Math.round(wp/10));
		result.add(-1);
		
		functionCall.getOutputCollector().add(result);
	}
	
	public String assignUserId(String adgroupId, String rawUserIdList, Map userMap){
		List<String> userIdList = null;
		String[] rawUserIds = null;
		String commonUser = null;
		String userId = null;
		
		// get the argument's TupleEntry
		if(rawUserIdList!=null&&userMap!=null&&rawUserIdList.length()>0&&userMap.size()>0){
			rawUserIds = StringUtils.split(rawUserIdList,",");		
			userIdList = (List<String>) userMap.get(adgroupId);
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
		return userId;
	}	
	
	public String assignId(List<String> idList){
		String catId = null;					
		if(idList!=null&&idList.size()>0){
			int numOfCats = idList.size();
			int rand_ind = ((int) Math.round(Math.random()*10000))%numOfCats;
			catId = (String) idList.get(rand_ind);
		}
		return catId;
	}		
}
