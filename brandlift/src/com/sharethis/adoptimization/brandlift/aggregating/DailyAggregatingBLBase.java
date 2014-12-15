package com.sharethis.adoptimization.brandlift.aggregating;

import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * This is the assembly to aggregate all brand lift data.
 */

public class DailyAggregatingBLBase extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(DailyAggregatingBLBase.class);
	private static final long serialVersionUID = 1L;
	
	public DailyAggregatingBLBase(Pipe[] allBLAssemblyHourly, String[] pipeNamesArr, String[] keyFieldsArr, 
			String[] dataFieldsArr, Configuration config) throws IOException, ParseException, Exception
	{
		try{				
		    Pipe[] baseBLAssembly = new Pipe[0];
		    
		    if(pipeNamesArr!=null&&pipeNamesArr.length>0&&keyFieldsArr!=null&&keyFieldsArr.length>0
		    		&&allBLAssemblyHourly!=null){	
		    	baseBLAssembly = new Pipe[pipeNamesArr.length];
		    	if (allBLAssemblyHourly!=null){		    	
		    		for(int i=0; i<pipeNamesArr.length; i++){
		    			String[] keyFieldsStr = StringUtils.split(keyFieldsArr[i], ",");
		    			Fields keyFieldsMore = new Fields();
		    			for(int j=0; j<keyFieldsStr.length; j++){
		    				keyFieldsMore = keyFieldsMore.append(new Fields(keyFieldsStr[j]));			    		
		    			}

		    			Fields[] inFields = new Fields[]{new Fields(dataFieldsArr[0]),new Fields(dataFieldsArr[1]), 
		    					new Fields(dataFieldsArr[2]), new Fields(dataFieldsArr[3])};
		    			Fields[] outFields = new Fields[]{new Fields(dataFieldsArr[0]),new Fields(dataFieldsArr[1]), 
		    					new Fields(dataFieldsArr[2]), new Fields(dataFieldsArr[3])};					
		    			baseBLAssembly[i] = new AggregatingBLData(allBLAssemblyHourly, pipeNamesArr[i], keyFieldsMore,
		    					inFields, outFields).getTails()[0];
		    		}
		    	}		
		    }
		    setTails(baseBLAssembly);		    	
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}		
}
