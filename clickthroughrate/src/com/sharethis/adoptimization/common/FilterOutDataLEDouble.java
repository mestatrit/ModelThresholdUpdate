package com.sharethis.adoptimization.common;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/** 
 * This is a filter to remove the data whose value is not bigger than the 'val'
 */

public class FilterOutDataLEDouble extends BaseOperation implements Filter
{
	private static final long serialVersionUID = 1L;
	private double val = 0;
	
	public FilterOutDataLEDouble(double val) {
		super(Fields.ALL);
		this.val = val;
	}

	public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall){
		// get the argument's TupleEntry
		TupleEntry arguments = filterCall.getArguments();
		boolean isRemove = false;
		double x = 0;
		String var = arguments.getString(0);
		if(var!=null&&!"null".equalsIgnoreCase(var)&&!var.isEmpty()){
			x = Double.parseDouble(var);
		}
		if(x <= val){
			isRemove = true;
		}
		return isRemove;
	}
}
