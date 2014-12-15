package com.sharethis.adoptimization.clickthroughrate.ctrmodel;


import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AssigningOneConstantId;
import com.sharethis.adoptimization.common.FilterOutDataLEDouble;

/**
 * This is the assembly to read price confirmation data.
 */

public class CTRPFilteringData extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(CTRPFilteringData.class);
	private static final long serialVersionUID = 1L;
	
	public CTRPFilteringData(Pipe inAssembly, Fields keyFields, int thresholdVal, 
			Fields newField) throws Exception{
		try{		    
		    //Separating the rows with sum_imps<=thresholdVal;
		    Filter<?> filter = new FilterOutDataLEDouble(thresholdVal);
		    inAssembly = new Each(inAssembly, new Fields("sum_imps"), filter); 
	    	
	    	int newIdVal = 1;
		    Function<?> aFunc = new AssigningOneConstantId(newField, newIdVal);
		    inAssembly = new Each(inAssembly, aFunc, Fields.ALL); 
	    	Fields keptFields = keyFields.append(newField);
	    	keptFields = keptFields.append(new Fields("sum_imps", "ctr"));
	    	inAssembly = new Each(inAssembly, keptFields, new Identity());
		    
			setTails(inAssembly);			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
}
