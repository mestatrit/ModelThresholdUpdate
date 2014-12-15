package com.sharethis.adoptimization.pricevolume;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

/**
 * This is the class to generate the daily aggregated data.
 */

public class HourlyDataAggregationAssembly extends SubAssembly
{	

	private static final long serialVersionUID = 1L;
	private static final Logger sLogger = Logger.getLogger(HourlyDataAggregationAssembly.class);	
	
	public HourlyDataAggregationAssembly(Map<String, Tap> sources, String bidFilePath, String priceFilePath, String hourFolder, 
			String pDateStr, int hour, Configuration config, String pipeNameList, String keyFieldsList) throws Exception{
		try{			
			sLogger.info("Entering HourlyDataAggregationAssembly ... ");
			String[] keyFieldStr = StringUtils.split(keyFieldsList,';');
			String[] pipeNames = StringUtils.split(pipeNameList,';');
			int pipeLen = keyFieldStr.length;
			
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			sLogger.info("dayStr: " + dayStr);			
			
            int nLen = 2;
            Scheme scheme = new TextLine();
			JobConf jobConf = new JobConf(config);
            // The change is to handle the new data path: yyyyMMddhh
			String bidFilePathDate = bidFilePath + dayStr;
            String priceFilePathDate = priceFilePath + dayStr;            
			String bidFilePathHourly = bidFilePathDate + hourFolder;
			String priceFilePathHourly = priceFilePathDate + hourFolder;
			Tap bidTap = new Hfs(scheme, bidFilePathHourly);
			Tap priceTap = new Hfs(scheme, priceFilePathHourly);
			if(bidTap.resourceExists(jobConf)&&priceTap.resourceExists(jobConf)){
				sLogger.info(priceFilePathHourly + " and " + bidFilePathHourly + " exist.");
				Pipe[] allAssembly = (new HourlyPriceBidSubAssembly(sources, bidFilePath, priceFilePath, pDateStr, hour, hourFolder, config)).getTails();												           

				Pipe[] pvAssembly = new Pipe[nLen*pipeLen+1];
				pvAssembly[nLen*pipeLen] = new Pipe("rtb_success_bid", allAssembly[1]);
				for(int i_p=0; i_p<pipeLen; i_p++){
					sLogger.info("i_p = " + i_p + "   pipeNames = " + pipeNames[i_p] + "    keyFields = " + keyFieldStr[i_p]);
					Pipe[] wbAssembly = (new HourlyAggregatingDataBySegment(allAssembly, pipeNames[i_p], keyFieldStr[i_p], pDateStr)).getTails();
					for(int i_n=0; i_n<nLen; i_n++)
						pvAssembly[i_n+i_p*nLen] = wbAssembly[i_n];				
				}
				setTails(pvAssembly);
			}else{
				sLogger.info(priceFilePathHourly + " or " + bidFilePathHourly + " does not exist.");					
			}
			sLogger.info("Exiting from HourlyDataAggregationAssembly.");			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  
}
