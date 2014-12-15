package com.sharethis.adoptimization.common;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/** 
 * This is a filter to remove the data whose value is greater than the 'val'
 */

public class FilterOutNullData extends BaseOperation implements Filter
{
	private static final long serialVersionUID = 1L;
	
	public FilterOutNullData() {
		super(Fields.ALL);
	}

	public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall){
		// get the argument's TupleEntry
		TupleEntry arguments = filterCall.getArguments();
		boolean isRemove = false;
		if(arguments.getString(0)==null || (arguments.getString(0)).isEmpty()
				|| "".equalsIgnoreCase(arguments.getString(0))
				|| "null".equalsIgnoreCase(arguments.getString(0))){
			isRemove = true;
		}
		return isRemove;
	}
}
