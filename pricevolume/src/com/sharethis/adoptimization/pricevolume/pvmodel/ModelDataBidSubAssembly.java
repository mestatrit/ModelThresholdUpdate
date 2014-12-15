package com.sharethis.adoptimization.pricevolume.pvmodel;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;


/**
 * This is the assembly to read win data for modeling.
 */

public class ModelDataBidSubAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(ModelDataBidSubAssembly.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
	
	public ModelDataBidSubAssembly(Map<String, Tap> sources, 
			String outFilePath, String pDateStr, Configuration config, 
			Fields[] keyFields, Fields[] pFields, String[] pipeNames, Class[][] types, int numOfDays) 
		throws IOException, ParseException, Exception
	{
		try{		
			int pipeLen = pipeNames.length;
			Pipe[] winAssembly = new Pipe[pipeLen];
			
			JobConf jobConf = new JobConf(config);
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			String outFilePathDate = outFilePath + dayStr + "/";
			for(int i_p=0; i_p<pipeLen; i_p++){
				String pipeName = pipeNames[i_p] + "_bid_daily";
				if(numOfDays == 7)
					pipeName = pipeNames[i_p] + "_bid_agg";
				winAssembly[i_p] = new Pipe(pipeName);
				// Loading the daily win data
				String pFileName = outFilePathDate + pipeName;
				Scheme pSourceScheme = new TextDelimited(pFields[i_p], false, mDelimiter, types[i_p]);
				Tap pFileTap = new Hfs(pSourceScheme, pFileName);
				if(pFileTap.resourceExists(jobConf)){
					sLogger.info("The file: " + pFileName + " exists!");				          
					sources.put(pipeName, pFileTap);					
				}else{
					sLogger.info("The file: " + pFileName + " does not exists!");						
				}					
			}
			setTails(winAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
