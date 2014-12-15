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

public class FilterOutDataNEInt extends BaseOperation implements Filter
{
	private static final Logger sLogger = Logger.getLogger(FilterOutDataNEInt.class);
	private static final long serialVersionUID = 1L;
	private int val = 1;
	
	public FilterOutDataNEInt(int val) {
		super(Fields.ALL);
		this.val = val;
	}

	public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall){
		// get the argument's TupleEntry
		TupleEntry arguments = filterCall.getArguments();
		boolean isRemove = true;
		sLogger.info("val: " + val + "    arg: " + arguments.getString(0));
		if(val == Integer.parseInt(arguments.getString(0))){
			isRemove = false;
		}
		return isRemove;
	}
}
