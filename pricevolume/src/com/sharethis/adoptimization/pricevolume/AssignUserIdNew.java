package com.sharethis.adoptimization.pricevolume;

import java.util.ArrayList;
import java.util.List;

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

public class AssignUserIdNew extends BaseOperation implements Function
{
	private static final long serialVersionUID = 1L;
	
	public AssignUserIdNew(Fields fields) {
		super(1, fields);
	}

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		String userListStr = null;
		String userId = null;
		
		// get the argument's TupleEntry
		TupleEntry arguments = functionCall.getArguments();
		userListStr = arguments.getString(0);
		if(userListStr != null && !userListStr.isEmpty() && !"null".equalsIgnoreCase(userListStr)){
			if(userListStr.contains("|")){
				String[] userListPairs = StringUtils.split(userListStr, "|");
				if(userListPairs != null && userListPairs.length>0){
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
					if(numOfUsers>0){
						int rand_ind = ((int) Math.round(Math.random()*10000))%numOfUsers;
						userId = keyVals.get(rand_ind);
					}
				}
			}
		}
		Tuple result = new Tuple();
		result.add(userListStr);
		result.add(userId);
		functionCall.getOutputCollector().add(result);
    }
}
