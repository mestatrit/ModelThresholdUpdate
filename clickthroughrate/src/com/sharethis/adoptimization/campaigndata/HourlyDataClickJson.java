package com.sharethis.adoptimization.campaigndata;

import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Unique;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.MultiSourceTap;
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
import com.sharethis.adoptimization.common.FilterOutDataWithStr;
import com.sharethis.adoptimization.common.FilterOutNullData;
import com.sharethis.adoptimization.common.ParsingJsonFormatClickData;


/**
 * This is the assembly to read price confirmation data.
 */

public class HourlyDataClickJson extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataClickJson.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public HourlyDataClickJson(Map<String, Tap> sources, String filePath, String pDateStr, 
			int hour, int numOfHoursClick, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{		
			// Defining the fields of price data
            Fields clickFields = new Fields("click_cookie_id", "clickJson");	
            //Fields clickFields = new Fields("clickJson");	
            Class[] types = new Class[]{String.class, String.class};
            
    		Date pDate = sdf.parse(pDateStr);
            //Scheme clickSourceScheme = new TextDelimited(clickFields);
            Scheme clickSourceScheme = new TextDelimited(clickFields, false, mDelimiter, types);
            //Scheme clickSourceScheme = new TextLine(clickFields);
            List<Tap> clickTapL = new ArrayList<Tap>();
            int ind = 0;
            for(int i=0; i<numOfHoursClick; i++){
            	int i_temp = i+hour-1;
        		Date pDate_temp = DateUtils.addHours(pDate, i_temp);
        		String pDateStr_temp = sdf.format(pDate_temp);
        		String dayStr_temp = pDateStr_temp.substring(0,4)+pDateStr_temp.substring(5,7)+pDateStr_temp.substring(8,10);
				if (i_temp > 23) {					
					i_temp = i_temp-24;
				}else{
					if(i_temp < 0)
						i_temp = 24 + i_temp;
				}
//				String filePathHourly = filePath + dayStr_temp + "/" + CTRConstants.hourFolders[i_temp] + "/";
				String filePathHourly = filePath + dayStr_temp + CTRConstants.hourFolders[i_temp] + "/data/";
           	    // Building the taps from the data files
            	Tap cTap = new Hfs(clickSourceScheme, filePathHourly);
    			JobConf	jobConf = new JobConf();
    			if(cTap.resourceExists(jobConf)){
    				sLogger.info(filePathHourly + " is used.");
    				clickTapL.add(ind, cTap);
    				ind++;
    			}else{
    				sLogger.info("The data path: " + filePathHourly + " does not exist.");				    				
    			}    				
            }	
            int tapLen = clickTapL.size();
            Pipe clickAssembly = null;
            if(tapLen>0){
            	String pipeName = filePath + CTRConstants.hourFolders[hour];	
            	clickAssembly = new Pipe(pipeName);
            	Tap[] clickTap = new Tap[tapLen];
            	for(int i=0; i<tapLen; i++)
            		clickTap[i] = clickTapL.get(i);
            	// Defining source Taps
				MultiSourceTap cSources = new MultiSourceTap(clickTap);
				sources.put(pipeName, cSources);
            }else{
            	sLogger.info("ProcessingDate: " + pDateStr + " and the hour: " + hour);
            }
            
            Fields jsonField = new Fields("clickJson");
	    	clickAssembly = new Each(clickAssembly, jsonField, new Identity());
		
            //Parsing json format to the data fields for the pipe.
            Function<?> jsonParsing = new ParsingJsonFormatClickData(new Fields(CTRConstants.clkNames), CTRConstants.clkJsonNames);
			clickAssembly = new Each(clickAssembly, jsonField, jsonParsing, Fields.RESULTS); 

			if(clickAssembly != null){
				Filter<?> filterOutNull = new FilterOutDataWithStr("Crawler");
				clickAssembly = new Each(clickAssembly, new Fields("click_user_agent"), filterOutNull); 
				clickAssembly = new Unique(clickAssembly, new Fields("click_jid"), 100000);
			}

			setTails(clickAssembly);            
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
