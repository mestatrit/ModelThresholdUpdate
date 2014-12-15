package com.sharethis.adoptimization.clickthroughrate;

import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.DataWeightedFunction;

/**
 * This is the assembly to append the weekly data over the number of weeks.
 */

public class HourlyAggregationAssemblyRolling extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyAggregationAssemblyRolling.class);
	private static final long serialVersionUID = 1L;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public HourlyAggregationAssemblyRolling(Map<String, Tap> sources, String outFilePathHourly, String outFilePathDaily, 
			String pDateStr, int interval1, int interval2, Fields pFields, String pipeName, Class[] types, int hour, 
			String outfile_postfix, String infile_postfix_rolling, int numOfKeyFields, int numOfDataFields) 
		throws IOException, ParseException, Exception
	{
		try{		
			Pipe[] ctrAssemblyRolling = null;
			Pipe[] ctrAssemblyRolling_tmp = new Pipe[3];
			int cnt_pipe =0;
			Date iDate = null;
			String iDateStr = null;
			ctrAssemblyRolling_tmp[0] = new OneCTRDataAssembly(sources, outFilePathHourly, outFilePathDaily, 
					pDateStr, pFields, pipeName, types, hour, infile_postfix_rolling, 0).getTails()[0];
			
			if(ctrAssemblyRolling_tmp[0]!=null){	
				sLogger.info(ctrAssemblyRolling_tmp[0].getName() + " is not null!");
				cnt_pipe++;
				iDate = DateUtils.addDays(sdf.parse(pDateStr), -interval1);
				iDateStr = sdf.format(iDate);
				ctrAssemblyRolling_tmp[1] = new OneCTRDataAssembly(sources, outFilePathHourly, outFilePathDaily, 
						iDateStr, pFields, pipeName, types, hour, outfile_postfix, 1).getTails()[0];

				if(ctrAssemblyRolling_tmp[1]!=null){
					sLogger.info(ctrAssemblyRolling_tmp[1].getName() + " is not null!");
					cnt_pipe++;
					if(interval2>12){
						Function<?> wFunc = new DataWeightedFunction(pFields, pFields, numOfKeyFields, numOfDataFields, 0.99);
						ctrAssemblyRolling_tmp[1] = new Each(ctrAssemblyRolling_tmp[1], pFields, wFunc, Fields.RESULTS);
					}else{
						iDate = DateUtils.addDays(sdf.parse(pDateStr), -interval2);
						iDateStr = sdf.format(iDate);
						ctrAssemblyRolling_tmp[2] = new OneCTRDataAssembly(sources, outFilePathHourly, outFilePathDaily, 
								iDateStr, pFields, pipeName, types, hour, infile_postfix_rolling, 2).getTails()[0];
						if(ctrAssemblyRolling_tmp[2]!=null){
							sLogger.info(ctrAssemblyRolling_tmp[2].getName() + " is not null!");
							cnt_pipe++;
							Function<?> wFunc = new DataWeightedFunction(pFields, pFields, numOfKeyFields, numOfDataFields, -1);
							ctrAssemblyRolling_tmp[2] = new Each(ctrAssemblyRolling_tmp[2], pFields, wFunc, Fields.RESULTS);
						}
					}
				}			    
				ctrAssemblyRolling = new Pipe[cnt_pipe];
				for(int i=0; i<cnt_pipe; i++)
					ctrAssemblyRolling[i] = ctrAssemblyRolling_tmp[i];
			}
			setTails(ctrAssemblyRolling);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
