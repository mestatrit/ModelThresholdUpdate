package com.sharethis.adoptimization.brandlift.aggregating;

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

public class HourlyAggregatingBLAll extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyAggregatingBLAll.class);
	private static final long serialVersionUID = 1L;
	
	public HourlyAggregatingBLAll(Pipe baseBLAssembly, String[] pipeNamesArr, String[] keyFieldsArr, 
			Configuration config) throws IOException, ParseException, Exception
	{
		try{				
		    Pipe[] sumAssemblys = new Pipe[0];
		    
		    if(pipeNamesArr!=null&&pipeNamesArr.length>0&&keyFieldsArr!=null&&keyFieldsArr.length>0
		    		&&baseBLAssembly!=null){	
		    	sumAssemblys = new Pipe[pipeNamesArr.length];
		    	if (baseBLAssembly!=null){		    	
		    		for(int i=0; i<pipeNamesArr.length; i++){
		    			String[] keyFieldsStr = StringUtils.split(keyFieldsArr[i], ",");
		    			Fields keyFieldsMore = new Fields();
		    			for(int j=0; j<keyFieldsStr.length; j++){
		    				keyFieldsMore = keyFieldsMore.append(new Fields(keyFieldsStr[j]));			    		
		    			}

		    			Fields[] inFields = new Fields[]{new Fields("sum_imps1"),new Fields("sum_clicks1"), 
		    					new Fields("sum_cost1"), new Fields("sum_vote1")};					
		    			Fields[] outFields = new Fields[]{new Fields("sum_imps"),new Fields("sum_clicks"), 
		    					new Fields("sum_cost"), new Fields("sum_vote")};					
		    			sumAssemblys[i] = new AggregatingBLData(new Pipe[]{baseBLAssembly}, pipeNamesArr[i], keyFieldsMore,
		    					inFields, outFields).getTails()[0];
		    		}
		    	}			
		    }
		    setTails(sumAssemblys);		    	
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}		
}
