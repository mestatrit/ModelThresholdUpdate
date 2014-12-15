package com.sharethis.adoptimization.brandlift.parsing;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.BLConstants;
import com.sharethis.adoptimization.common.ConfigurationUtil;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class HourlyParsingBLDataTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(HourlyParsingBLDataTask.class);
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public int run(String[] args) throws Exception {
		sLogger.info("Getting the configuration ...");
		Configuration config = ConfigurationUtil.setParms(args);
		sLogger.info("Getting the configuration is done.");	
		int offset = 0;
		int hour = 0;
		if(config.get("HourOfDay")==null){
			// If HourOfDay is null, then using the last hour as the processing hour.
			Calendar calendar = Calendar.getInstance();
		    hour = calendar.get(Calendar.HOUR_OF_DAY)-1;
			if (hour<0) {
				hour = 24+hour;
				offset = 1;
			}
			config.setInt("HourOfDay", hour);
		}
		
		if(config.get("ProcessingDate")==null){
			// If ProcessDate is null, then using the date of yesterday as the processing date.
			Date date = DateUtils.addDays(new Date(), -offset);
			config.set("ProcessingDate", sdf.format(date));
		}
		//Performing the list of all tasks at a hour level
		hour = config.getInt("HourOfDay", hour);
		String pDateStr = config.get("ProcessingDate");
		String hourFolder = BLConstants.hourFolders[hour];
		HourlyParsingBLData hgData = new HourlyParsingBLData();
		sLogger.info("\nStarting the generation of hourly aata at hour " + hourFolder + " on " + pDateStr + " ...");
		hgData.parsingBLDataHourly(config, hour);
		sLogger.info("The generation of hourly data at hour " + hourFolder + " on " + pDateStr + " is done.\n");
		return 0;		
	}		
}
