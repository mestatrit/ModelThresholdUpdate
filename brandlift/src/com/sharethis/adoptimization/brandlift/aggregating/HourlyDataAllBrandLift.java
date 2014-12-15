package com.sharethis.adoptimization.brandlift.aggregating;

import cascading.operation.Filter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.joiner.LeftJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.BLConstants;
import com.sharethis.adoptimization.common.FilterOutDataNEStrArr;
import com.sharethis.adoptimization.common.FilterOutNullData;

/**
 * This is the assembly to join the all data and brand lift data.
 */

public class HourlyDataAllBrandLift extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataAllBrandLift.class);
	private static final long serialVersionUID = 1L;

	public HourlyDataAllBrandLift(Map<String, Tap> sources, String allFilePath, 
			String allFileName, String blFilePath, String blFileName, String pDateStr, 
			int hour, Configuration config) throws IOException, ParseException, Exception
	{
		try{				
		    Pipe[] allAssembly = new HourlyDataAll(sources, allFilePath, allFileName, 
		    		pDateStr, hour, config).getTails();
		    
		    Pipe blAssembly = new HourlyDataBrandLift(sources, blFilePath, blFileName, pDateStr, 
					hour, config).getTails()[0];

		    Pipe allBLAssembly = null;
		    Pipe allBLAssemblyResp = null;
		    
		    if (!(allAssembly==null) && !(blAssembly==null)){
		    	
		    	// Building the pipe to join the data
		    	Fields icKeyFields = new Fields("jid");
		    	Fields blKeyFields = new Fields("retarg_jid");
		    	//Joining icAssembly and sbAssembly
				allBLAssembly = new CoGroup("all_bl_data", allAssembly[0], icKeyFields, blAssembly, blKeyFields, new LeftJoin());

				allBLAssemblyResp = new Pipe("all_bl_resp_data", allBLAssembly);
				Filter<?> filterOutNull = new FilterOutNullData();
				allBLAssemblyResp = new Each(allBLAssemblyResp, new Fields("retarg_jid"), filterOutNull); 
		    }				
					    
		    setTails(allBLAssembly, allAssembly[0], allBLAssemblyResp);		    	
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
