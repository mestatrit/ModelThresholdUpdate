package com.sharethis.adoptimization.inventory;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;

/**
 * This is the class to generate the daily aggregated data.
 */

public class HourlyInventorySubAssembly extends SubAssembly
{	
	private static final long serialVersionUID = 1L;
	private static final Logger sLogger = Logger.getLogger(HourlyInventorySubAssembly.class);	
	
	public HourlyInventorySubAssembly(Map<String, Tap> sources, String nobidFilePath, String hourFolder, 
			String pDateStr, int hour, Configuration config, String basePipeName, String baseKeyFields, 
			String pipeNameList, String keyFieldsList) throws Exception{
		try{			
			sLogger.info("Entering HourlyInventorySubAssembly ... ");
			String[] keyFieldStr = StringUtils.split(keyFieldsList,';');
			String[] pipeNames = StringUtils.split(pipeNameList,';');
			int pipeLen = keyFieldStr.length;
			Pipe[] nbAssembly = new Pipe[pipeLen+1];
			Pipe dataAssembly = new HourlyInventory(sources, nobidFilePath, pDateStr, hour, config).getTails()[0];	
			//nbAssembly[pipeLen+1] = new Pipe("inventory_data", dataAssembly);
			Pipe nbAssembly_tmp = new HourlyInventoryAggregation(dataAssembly, basePipeName+"_tmp", baseKeyFields).getTails()[0];
			nbAssembly[pipeLen] = new Pipe(basePipeName, nbAssembly_tmp);
			if(nbAssembly_tmp!=null){
				for(int i_p=0; i_p<pipeLen; i_p++){
					sLogger.info("i_p = " + i_p + "   pipeNames = " + pipeNames[i_p] + "    keyFields = " + keyFieldStr[i_p]);
					Fields keyFields = new Fields();
					String[] keyFieldStr_tmp = StringUtils.split(keyFieldStr[i_p],",");
					for (int i=0; i< keyFieldStr_tmp.length; i++){
						keyFields = keyFields.append(new Fields(keyFieldStr_tmp[i]));
					}
					nbAssembly[i_p] = new AggregatingInventory(new Pipe[]{nbAssembly_tmp}, pipeNames[i_p], keyFields).getTails()[0];
				}
			}
			setTails(nbAssembly);
			sLogger.info("Exiting from HourlyInventorySubAssembly.");			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  
}
