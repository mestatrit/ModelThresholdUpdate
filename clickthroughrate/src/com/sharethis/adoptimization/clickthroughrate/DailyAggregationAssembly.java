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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.CTRConstants;


/**
 * This is the assembly to append the hourly data over the number of hours.
 */

public class DailyAggregationAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(DailyAggregationAssembly.class);
	private static final long serialVersionUID = 1L;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static String mDelimiter = "\t";
	
	public DailyAggregationAssembly(Map<String, Tap> sources, 
			String outFilePath, String pDateStr, int numOfHours, int interval, Configuration config, 
			Fields pFields, String pipeName, int hour, Class[] types, String infile_postfix) 
		throws IOException, ParseException, Exception
	{
		try{		
			Date iDate = null;
			String iDateStr = null;

			List<Tap> pSourceList = new ArrayList<Tap>();
			
			JobConf jobConf = new JobConf();
			for(int i=0; i<numOfHours; i++){
				int hour_temp = hour - i*interval;
				iDate = DateUtils.addHours(sdf.parse(pDateStr), hour_temp);
				iDateStr = sdf.format(iDate);
				String dayStr = iDateStr.substring(0,4)+iDateStr.substring(5,7)+iDateStr.substring(8,10);
//				String outFilePathDate = outFilePath + dayStr + "/";
				String outFilePathDate = outFilePath + dayStr;
				while(hour_temp < 0)
					hour_temp = hour_temp + 24;
				outFilePathDate = outFilePathDate + CTRConstants.hourFolders[hour_temp] + "/";
				String pFileName = outFilePathDate + pipeName + infile_postfix;

				Scheme pSourceScheme = new TextDelimited(pFields, false, mDelimiter, types);
				Tap pFileTap = new Hfs(pSourceScheme, pFileName);
				if(pFileTap.resourceExists(jobConf)){
					sLogger.info("The file: " + pFileName + " exists!");	
			        pSourceList.add(pFileTap);
				}else{
					sLogger.info("The file: " + pFileName + " does not exists!");						
				}					
			}

			int	sourceLen = pSourceList.size();
			Pipe[] ctrAssembly = new Pipe[sourceLen];
			if(sourceLen > 0){
				sLogger.info("The ending hour: " + hour +  "     The number of hours used: " + sourceLen);
				for(int i=0; i<sourceLen; i++){
					Tap pSource = pSourceList.get(i);
					String pipeName_i = pipeName + i;
					ctrAssembly[i] = new Pipe(pipeName_i);
					sources.put(pipeName_i, pSource);
				}
			}else{
				sLogger.info("The ending hour: " + hour + "     The number of hours: " + numOfHours + " with an interval: " + interval);
				throw new Exception("No hourly data for the aggregation over the hour range exists.");
			}	
			setTails(ctrAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
