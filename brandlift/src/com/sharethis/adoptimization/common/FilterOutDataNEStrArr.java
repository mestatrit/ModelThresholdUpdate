package com.sharethis.adoptimization.common;

import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/** 
 * This is a filter to remove the data whose value is not equal to the 'val'
 */

public class FilterOutDataNEStrArr extends BaseOperation implements Filter
{
	private static final Logger sLogger = Logger.getLogger(FilterOutDataNEStrArr.class);
	private static final long serialVersionUID = 1L;
	private String[] val1 = null;
	
	public FilterOutDataNEStrArr(String[] val1) {
		super(Fields.ALL);
		this.val1 = val1;
	}

	public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall){
		// get the argument's TupleEntry
		TupleEntry arguments = filterCall.getArguments();
		boolean isRemove = true;
		sLogger.info("val1: " + val1 + "    arg: " + arguments.getString(0));
		for(int i=0; i<val1.length; i++){
			if(val1[i].equalsIgnoreCase(arguments.getString(0))){
				isRemove = false;
			}
		}
		return isRemove;
	}
}
