package com.sharethis.adoptimization.pricevolume;

import cascading.operation.Buffer;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
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

public class AppendingDailyPriceDataSubAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(AppendingDailyPriceDataSubAssembly.class);
	private static final long serialVersionUID = 1L;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static String mDelimiter = "\t";
	
	public AppendingDailyPriceDataSubAssembly(Map<String, Tap> sources, 
			String outFilePath, String pDateStr, int numOfDays, Configuration config, 
			Fields[] keyFields, Fields[] pFields, String[] pipeNames, Class[][] types) 
		throws IOException, ParseException, Exception
	{
		try{		
			// Defining the kept fields after price and bidder are merged.
				
			Date iDate = null;
			String iDateStr = null;
			int pipeLen = pipeNames.length;

			List[] pSourceList = new ArrayList[pipeLen];
			for(int i_p=0; i_p<pipeLen; i_p++)
				pSourceList[i_p] = new ArrayList<Tap>();
			
			JobConf jobConf = new JobConf(config);
			for(int i=0; i<numOfDays; i++){
				iDate = DateUtils.addDays(sdf.parse(pDateStr), -numOfDays+i+1);
				iDateStr = sdf.format(iDate);
				String dayStr = iDateStr.substring(0,4)+iDateStr.substring(5,7)+iDateStr.substring(8,10);
				String outFilePathDate = outFilePath + dayStr + "/";
				for(int i_p=0; i_p<pipeLen; i_p++){
					// Loading the daily win data
					String pFileName = outFilePathDate + pipeNames[i_p] + "_win_daily";
					Scheme pSourceScheme = new TextDelimited(pFields[i_p], false, mDelimiter, types[i_p]);
					Tap pFileTap = new Hfs(pSourceScheme, pFileName);
					if(pFileTap.resourceExists(jobConf)){
						sLogger.info("The file: " + pFileName + " exists!");	
						//Tap pTap = new Hfs(pSourceScheme, pFileName);
			            pSourceList[i_p].add(pFileTap);
					}else{
						sLogger.info("The file: " + pFileName + " does not exists!");						
					}					
				}
			}
			
			Pipe[] pAssembly = new Pipe[pipeLen];
			for(int i_p=0; i_p<pipeLen;i_p++){
				int	sourceLen = pSourceList[i_p].size();
				if(sourceLen > 0){
					sLogger.info("ProcessingDate: " + pDateStr + " and the number of days used: " + sourceLen);
					Tap[] pSource = new Tap[sourceLen];

					for(int i=0; i<sourceLen; i++){
						pSource[i] = (Tap) pSourceList[i_p].get(i);
					}
			
					// Defining source Taps
					MultiSourceTap pSources = new MultiSourceTap(pSource);
			
					// The 'head' of the pipe assembly
					String pPipeName = pipeNames[i_p]+"_win";
					Pipe pAssembly_temp = new Pipe(pPipeName);

					// Accumulating the wins for all price points.							
					// Aggregating the winning data grouped by the groupFields
					Fields groupFields = keyFields[i_p].append(new Fields("winning_price"));
			        // Now create a flow that does a a SumBy. 
					
			        SumBy sum1 = new SumBy(new Fields("sum_wins"), new Fields("sum_wins"), Integer.class); 
			        SumBy sum2 = new SumBy(new Fields("sum_mbids"), new Fields("sum_mbids"), Integer.class); 
			        pAssembly[i_p] = new AggregateBy(pAssembly_temp, groupFields, sum1, sum2); 
			        //pAssembly[i_p] = new Each(pAssembly[i_p], new Debug("sum_price", true)); 

					Fields sortField = new Fields("winning_price"); 
					Pipe pAssembly_cum = new GroupBy(pPipeName, pAssembly[i_p], keyFields[i_p], sortField); 
					Buffer<?> accum = new RunningCount(new Fields("cum_wins"));
					pAssembly[i_p] = new Every(pAssembly_cum, new Fields("sum_wins"), accum, Fields.ALL); 
//					Function<?> aValue = new AssignConstantValue(new Fields("num_of_days","date_"), sourceLen, pDateStr);
//					pAssembly[i_p] = new Each(pAssembly[i_p], aValue, Fields.ALL);			
								
					sources.put(pPipeName, pSources);
				}else{
					sLogger.info("ProcessingDate: " + pDateStr + " and NumOfDays: " + numOfDays);
					throw new Exception("No daily data for the day range does not exist.");
				}	
			}
			setTails(pAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
