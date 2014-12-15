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

public class HourlyAggregatingBLBase extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyAggregatingBLBase.class);
	private static final long serialVersionUID = 1L;
	
	public HourlyAggregatingBLBase(Pipe allBLAssembly, String[] pipeNamesArr, String[] keyFieldsArr, 
			Configuration config) throws IOException, ParseException, Exception
	{
		try{				
		    Pipe[] baseBLAssembly = new Pipe[0];
		    
		    if(pipeNamesArr!=null&&pipeNamesArr.length>0&&keyFieldsArr!=null&&keyFieldsArr.length>0
		    		&&allBLAssembly!=null){	
		    	baseBLAssembly = new Pipe[pipeNamesArr.length];
		    	if (allBLAssembly!=null){
		    	
		    		Function<?> voteFunc = new AssignVoteInt(new Fields("bl_opt", "bl_pos", "bl_vote"));
		    		allBLAssembly = new Each(allBLAssembly, new Fields("retarg_jid", "retarg_campaign_name"), voteFunc, Fields.ALL);
		    		for(int i=0; i<pipeNamesArr.length; i++){
		    			String[] keyFieldsStr = StringUtils.split(keyFieldsArr[i], ",");
		    			Fields keyFieldsMore = new Fields();
		    			for(int j=0; j<keyFieldsStr.length; j++){
		    				keyFieldsMore = keyFieldsMore.append(new Fields(keyFieldsStr[j]));			    		
		    			}

		    			Fields[] inFields = new Fields[]{new Fields("imp_flag1"),new Fields("click_flag1"), 
		    					new Fields("winning_price"), new Fields("bl_vote")};
		    			Fields[] outFields = new Fields[]{new Fields("sum_imps1"),new Fields("sum_clicks1"), 
		    					new Fields("sum_cost1"), new Fields("sum_vote1")};					
		    			baseBLAssembly[i] = new AggregatingBLData(new Pipe[]{allBLAssembly}, pipeNamesArr[i], keyFieldsMore,
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
