package com.sharethis.adoptimization.common;

import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/** 
 * This is a filter to remove the data whose value is greater than the 'val'
 */

public class FilterOutDataGEInt extends BaseOperation implements Filter
{
	private static final Logger sLogger = Logger.getLogger(FilterOutDataGEInt.class);
	private static final long serialVersionUID = 1L;
	private int val = 0;
	
	public FilterOutDataGEInt(int val) {
		super(Fields.ALL);
		this.val = val;
	}

	public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall){
		// get the argument's TupleEntry
		TupleEntry arguments = filterCall.getArguments();
		boolean isRemove = false;
		sLogger.info("val: " + val + "    arg: " + arguments.getString(0));
		int x = Integer.parseInt(arguments.getString(0));
		if(x >= val){
			isRemove = true;
		}
		return isRemove;
	}
}
