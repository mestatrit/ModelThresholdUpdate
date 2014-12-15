package com.sharethis.adoptimization.clickthroughrate;

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

import com.sharethis.adoptimization.common.CTRConstants;

/**
 * This is the assembly to append the weekly data over the number of weeks.
 */

public class HourlyBaseDataAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyBaseDataAssembly.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
	
	public HourlyBaseDataAssembly(Map<String, Tap> sources, String outFilePathHourly, String outFilePathDaily, String pDateStr, 
			Configuration config, Fields pFields, String pipeName, Class[] types, int hour, String file_postfix) 
		throws IOException, ParseException, Exception
	{
		try{		
			Pipe ctrAssembly = null;
			JobConf jobConf = new JobConf();
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
//			String outFilePathDate = outFilePath + dayStr + "/";
//			if(hour>=0){
//				outFilePathDate = outFilePathDate + CTRConstants.hourFolders[hour] + "/";
//			}
			String outFilePathDate = null;
			if(hour>=0){
				outFilePathDate = outFilePathHourly + dayStr + CTRConstants.hourFolders[hour] + "/";
			}else{
				outFilePathDate = outFilePathDaily + dayStr + "/";					
			}
			String pFileName = outFilePathDate + pipeName + file_postfix;
			Scheme pSourceScheme = new TextDelimited(pFields, false, mDelimiter, types);
			Tap pFileTap = new Hfs(pSourceScheme, pFileName);
			if(pFileTap.resourceExists(jobConf)){
				sLogger.info("The file: " + pFileName + " is used!");	
				ctrAssembly = new Pipe(pipeName);
				sources.put(pipeName, pFileTap);
			}else{
				sLogger.info("The file: " + pFileName + " does not exists!");						
			}					
			setTails(ctrAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
