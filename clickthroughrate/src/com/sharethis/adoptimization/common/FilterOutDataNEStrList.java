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

public class FilterOutDataNEStrList extends BaseOperation implements Filter
{
	private static final Logger sLogger = Logger.getLogger(FilterOutDataNEStrList.class);
	private static final long serialVersionUID = 1L;
	private String val1 = null;
	private String val2 = null;
	
	public FilterOutDataNEStrList(String val1, String val2) {
		super(Fields.ALL);
		this.val1 = val1;
		this.val2 = val2;
	}

	public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall){
		// get the argument's TupleEntry
		TupleEntry arguments = filterCall.getArguments();
		boolean isRemove = true;
		sLogger.info("val1: " + val1 + "    arg: " + arguments.getString(0));
		if(val1.equalsIgnoreCase(arguments.getString(0))&&val2.equalsIgnoreCase(arguments.getString(1))){
			isRemove = false;
		}
		return isRemove;
	}
}
