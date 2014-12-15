package com.sharethis.adoptimization.brandlift.aggregating;

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


/**
 * This is the assembly to append the hourly data over the number of hours.
 */

public class PeriodicalUploadingBLData extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(PeriodicalUploadingBLData.class);
	private static final long serialVersionUID = 1L;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static String mDelimiter = "\t";
	
	public PeriodicalUploadingBLData(Map<String, Tap> sources, String outFilePath, 
			String pDateStr, int numOfDays, int startDay, Configuration config, 
			Fields pFields, String pipeName, Class[] types, String infile_postfix) 
		throws IOException, ParseException, Exception
	{
		Pipe[] blAssembly = null;
		try{		
			Date iDate = null;
			String iDateStr = null;

			List<Tap> pSourceList = new ArrayList<Tap>();
			
			JobConf jobConf = new JobConf();
			for(int i=0; i<numOfDays; i++){
				iDate = DateUtils.addDays(sdf.parse(pDateStr), -i-startDay);
				iDateStr = sdf.format(iDate);
				String dayStr = iDateStr.substring(0,4)+iDateStr.substring(5,7)+iDateStr.substring(8,10);
				String outFilePathDate = outFilePath + dayStr + "/";
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
			if(sourceLen > 0){
				blAssembly = new Pipe[sourceLen];
				sLogger.info("The number of days used: " + sourceLen);
				for(int i=0; i<sourceLen; i++){
					Tap pSource = pSourceList.get(i);
					String pipeName_i = pipeName + infile_postfix + "_" + i;
					blAssembly[i] = new Pipe(pipeName_i);
					sources.put(pipeName_i, pSource);
				}
			}else{
				sLogger.info("There are no " + infile_postfix + " data over the number of the days: " + numOfDays + " on " + pDateStr);
//				throw new Exception("No daily data for the aggregation over the day range exists.");
				blAssembly = new Pipe[1];
				blAssembly[0] = new Pipe("No_Data", null);
			}	
			setTails(blAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
