package com.sharethis.adoptimization.clickthroughrate;

import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;
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
 * This is the assembly to read the data.
 */

public class ModDailyNonClick extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(ModDailyNonClick.class);
	private static final long serialVersionUID = 1L;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
	public ModDailyNonClick(Map<String, Tap> sources, String ctrFilePath, Fields dFields,
			Class[] types, String pDateStr, Configuration config, String pipeName, 
			int numOfHours, int hour, String infile_postfix, 
			Map<String, List<String>> setCatMap, Map<String, List<String>> crtvCatMap,
			Fields newCatFields) 
		throws IOException, ParseException, Exception
	{
		try{		
			ModReadingCountData mRead = new ModReadingCountData();
			int sampMult = config.getInt("SampleMultiplier", 4);
			int total_clicks = 0;

			Date iDate = null;
			String iDateStr = null;

			List<Pipe> pipeList = new ArrayList<Pipe>();
			int i_cnt = 0;
			JobConf jobConf = new JobConf();
			for(int i=0; i<numOfHours; i++){
				int hour_temp = hour - i;
				iDate = DateUtils.addHours(sdf.parse(pDateStr), hour_temp);
				iDateStr = sdf.format(iDate);
				String dayStr = iDateStr.substring(0,4)+iDateStr.substring(5,7)+iDateStr.substring(8,10);
				String outFilePathDate = ctrFilePath + dayStr;
				while(hour_temp < 0)
					hour_temp = hour_temp + 24;
				outFilePathDate = outFilePathDate + CTRConstants.hourFolders[hour_temp] + "/";
				
				int[] cntClick = mRead.readingCountData(config, outFilePathDate);
				if(cntClick[0]>0){				
					total_clicks += cntClick[1];
					int numBase = Math.max(cntClick[1], 1) * sampMult;
					int maxRandNum = cntClick[0]/numBase;
					sLogger.info("The Numbers: " + cntClick[0] +  ", " + cntClick[1] + ", " + numBase + ", " + maxRandNum);
					String pFileName = outFilePathDate + pipeName + infile_postfix;
					Pipe hAssembly = new ModLoadingHourlyNonClick(sources, ctrFilePath, pDateStr, dFields, 
							types, config, pipeName+"_"+i, pFileName, maxRandNum,  
							setCatMap, crtvCatMap, newCatFields).getTails()[0];
					if(hAssembly!=null&&!(hAssembly.getName()).equalsIgnoreCase("No_Data")){
						pipeList.add(i_cnt, hAssembly);
						i_cnt++;
					}
				}
			}
			sLogger.info("The total number of clicks: " + total_clicks +  " on " + iDateStr);
			int	sourceLen = pipeList.size();
			Pipe[] modAssembly = new Pipe[sourceLen];
			if(sourceLen > 0){
				sLogger.info("The ending hour: " + hour +  "     The number of hours used: " + sourceLen);
				for(int i=0; i<sourceLen; i++){
					modAssembly[i] = pipeList.get(i);
				}
			}else{
				sLogger.info("The ending hour: " + hour + "     The number of hours: " + numOfHours);
				throw new Exception("No hourly data for the aggregation over the hour range exists.");
			}	
			Pipe modDataAssembly = new Pipe(pipeName, new GroupBy(modAssembly, dFields));
			setTails(modDataAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
