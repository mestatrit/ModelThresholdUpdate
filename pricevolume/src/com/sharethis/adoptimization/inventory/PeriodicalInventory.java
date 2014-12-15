package com.sharethis.adoptimization.inventory;

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
 * This is the assembly to append the weekly data over the number of weeks.
 */

public class PeriodicalInventory extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(PeriodicalInventory.class);
	private static final long serialVersionUID = 1L;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static String mDelimiter = "\t";
	
	public PeriodicalInventory(Map<String, Tap> sources, 
			String outFilePath, String pDateStr, int numOfPeriods, int interval, Configuration config, 
			Fields pFields, String pipeName, Class[] types, int hour, String file_postfix) 
		throws IOException, ParseException, Exception
	{
		try{		
			// Defining the kept fields after price and bidder are merged.				
			Date iDate = null;
			String iDateStr = null;

			List<Tap> pSourceList = new ArrayList<Tap>();
			
			JobConf jobConf = new JobConf(config);
			for(int i=0; i<numOfPeriods; i++){
				iDate = DateUtils.addDays(sdf.parse(pDateStr), -i*interval);
				iDateStr = sdf.format(iDate);
				String dayStr = iDateStr.substring(0,4)+iDateStr.substring(5,7)+iDateStr.substring(8,10);
				String outFilePathDate = outFilePath + dayStr + "/";
				if(hour>=0){
					outFilePathDate = outFilePathDate + PVConstants.hourFolders[hour] + "/";
				}
				String pFileName = outFilePathDate + pipeName + file_postfix;
				Scheme pSourceScheme = new TextDelimited(pFields, false, mDelimiter, types);
				Tap pFileTap = new Hfs(pSourceScheme, pFileName);
				if(pFileTap.resourceExists(jobConf)){
					sLogger.info("The file: " + pFileName + " is used!");	
			        pSourceList.add(pFileTap);
				}else{
					sLogger.info("The file: " + pFileName + " does not exists!");						
				}					
			}

			int	sourceLen = pSourceList.size();
			Pipe[] dataAssembly = new Pipe[sourceLen];
			if(sourceLen > 0){
				sLogger.info("The processing date: " + pDateStr + " and the number of periods used: " + sourceLen 
						+ " with the interval: " + interval);
				for(int i=0; i<sourceLen; i++){
					Tap pSource = (Tap) pSourceList.get(i);
					String pipeName_i = pipeName + i;
					dataAssembly[i] = new Pipe(pipeName_i);
					sources.put(pipeName_i, pSource);
				}
			}else{
				sLogger.info("The processing date: " + pDateStr + " and the number of periods: " 
						+ numOfPeriods + " with the interval: " + interval);
				throw new Exception("No data for the period range exists.");
			}	
			setTails(dataAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
