package com.sharethis.adoptimization.pricevolume;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.MultiSourceTap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;

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
 * This is the assembly to append the daily data over the number of days.
 */

public class AppendingDailyBidDataSubAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(AppendingDailyBidDataSubAssembly.class);
	private static final long serialVersionUID = 1L;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static String mDelimiter = "\t";
	
	public AppendingDailyBidDataSubAssembly(){}
	
	public AppendingDailyBidDataSubAssembly(Map<String, Tap> sources, 
			String outFilePath, String pDateStr, int numOfDays, Configuration config, 
			Fields[] keyFields, Fields[] bFields, String[] pipeNames, Class[][] types) 
		throws IOException, ParseException, Exception
	{
		try{		
			// Defining the kept fields after price and bidder are merged.
				
			Date iDate = null;
			String iDateStr = null;
			int pipeLen = pipeNames.length;
			List[] bSourceList = new ArrayList[pipeLen];
			for(int i_p=0; i_p<pipeLen; i_p++)
				bSourceList[i_p] = new ArrayList<Tap>();
			JobConf jobConf = new JobConf(config);
			for(int i=0; i<numOfDays; i++){
				iDate = DateUtils.addDays(sdf.parse(pDateStr), -numOfDays+i+1);
				iDateStr = sdf.format(iDate);
				String dayStr = iDateStr.substring(0,4)+iDateStr.substring(5,7)+iDateStr.substring(8,10);
				String outFilePathDate = outFilePath + dayStr + "/";
				for(int i_p=0; i_p<pipeLen; i_p++){
					// Loading the daily bid data
					String bFileName = outFilePathDate + pipeNames[i_p] + "_bid_daily";
					Scheme bSourceScheme = new TextDelimited(bFields[i_p], false, mDelimiter, types[i_p]);
					Tap bFileTap = new Hfs(bSourceScheme, bFileName);
					if(bFileTap.resourceExists(jobConf)){
						sLogger.info("The file: " + bFileName + " exists!");	
			            bSourceList[i_p].add(bFileTap);
					}else{
						sLogger.info("The file: " + bFileName + " does not exists!");						
					}
				}
			}
			
			Pipe[] bAssembly = new Pipe[pipeLen];
			for(int i_p=0; i_p<pipeLen;i_p++){
				int	sourceLen = bSourceList[i_p].size();
				if(sourceLen > 0){
					sLogger.info("ProcessingDate: " + pDateStr + " and the number of days used: " + sourceLen);
					Tap[] bSource = new Tap[sourceLen];

					for(int i=0; i<sourceLen; i++){
						bSource[i] = (Tap) bSourceList[i_p].get(i);
					}
			
					// Defining source Taps
					MultiSourceTap bSources = new MultiSourceTap(bSource);
			
					// The 'head' of the pipe assembly
					String bPipeName = pipeNames[i_p]+"_bid";
					Pipe bAssembly_temp = new Pipe(bPipeName);					
	
			        SumBy sum1 = new SumBy(new Fields("sum_bids"), new Fields("sum_bids"), Integer.class); 
			        SumBy sum2 = new SumBy(new Fields("sum_nobids"), new Fields("sum_nobids"), Integer.class); 
			        bAssembly[i_p] = new AggregateBy(bAssembly_temp, keyFields[i_p], sum1, sum2); 
			        //bAssembly[i_p] = new Each(bAssembly[i_p], new Debug("sum_bids", true)); 
//					Function<?> aValue = new AssignConstantValue(new Fields("num_of_days","date_"), sourceLen, pDateStr);
//					bAssembly[i_p] = new Each(bAssembly[i_p], aValue, Fields.ALL);			
			        
					sources.put(bPipeName, bSources);
				}else{
					sLogger.info("ProcessingDate: " + pDateStr + " and NumOfDays: " + numOfDays);
					throw new Exception("No daily data for the day range does not exist.");
				}	
			}
			setTails(bAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
