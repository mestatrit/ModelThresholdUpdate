package com.sharethis.adoptimization.pricevolume;

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

import com.sharethis.adoptimization.common.PVConstants;


/**
 * This is the assembly to append the daily data over the number of days.
 */

public class HourlyPriceDataSubAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyPriceDataSubAssembly.class);
	private static final long serialVersionUID = 1L;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static String mDelimiter = "\t";
	
	public HourlyPriceDataSubAssembly(Map<String, Tap> sources, 
			String outFilePath, String pDateStr, String[] hourFolders, Configuration config, 
			Fields pFields, String pipeNameHour, Class[] types) 
		throws IOException, ParseException, Exception
	{
		try{		
			int startHour = config.getInt("StartHour", 8);
			// Defining the kept fields after price and bidder are merged.
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			int numOfHours = hourFolders.length;	
			Date pDate = sdf.parse(pDateStr);
			Date pDate_after = DateUtils.addDays(pDate, 1);
			String pDateStr_after = sdf.format(pDate_after);
			String dayStr_after = pDateStr_after.substring(0,4)+pDateStr_after.substring(5,7)+pDateStr_after.substring(8,10);
			List<Tap> pSourceList = new ArrayList<Tap>();
			JobConf jobConf = new JobConf(config);
			int i_cnt=0;
			for(int i=0; i<numOfHours; i++){
				int i_temp = i + startHour;
				if (i_temp > 23) {					
					i_temp = i_temp-24;
					dayStr = dayStr_after;
				}
				//String outFilePathDate = outFilePath + dayStr + "/" + hourFolders[i_temp] + "/";
				//The change is to have the hourly data path: yyyyMMddHH
				String outFilePathDate = outFilePath + dayStr + hourFolders[i_temp] + "/";
				// Loading the daily win data
				String pFileName = outFilePathDate + pipeNameHour + "_win_hourly";
				Scheme pSourceScheme = new TextDelimited(pFields, false, mDelimiter, types);
				Tap pFileTap = new Hfs(pSourceScheme, pFileName);
				if(pFileTap.resourceExists(jobConf)){
					sLogger.info("The file: " + pFileName + " exists!");	
			        pSourceList.add(i_cnt, pFileTap);
			        i_cnt++;
				}else{
					sLogger.info("The file: " + pFileName + " does not exists!");						
				}					
			}
			
			int	sourceLen = pSourceList.size();	
			Pipe[] sPipes = new Pipe[sourceLen];
			if(sourceLen > 0){
				sLogger.info("ProcessingDate: " + pDateStr + " and the number of hours used: " + sourceLen);
				for(int i=0; i<sourceLen; i++){
					String sPipeName = "win_"+ i;
					sPipes[i] = new Pipe(sPipeName);
					sources.put(sPipeName, (Tap) pSourceList.get(i));
				}			
			}else{
				sLogger.info("ProcessingDate: " + pDateStr + " and NumOfHours: " + numOfHours);
				throw new Exception("No hourly data for the hour range does not exist.");
			}
			setTails(sPipes);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
