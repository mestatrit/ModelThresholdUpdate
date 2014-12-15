package com.sharethis.adoptimization.pricevolume;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.sharethis.adoptimization.common.ConfigurationUtil;
import com.sharethis.adoptimization.common.PVConstants;
import com.sharethis.adoptimization.pricevolume.GeneratingPV;


public class HourlyGeneratingPVTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(HourlyGeneratingPVTask.class);
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public int run(String[] args) throws Exception {
		sLogger.info("Getting the configuration ...");
		Configuration config = ConfigurationUtil.setConf(args);
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
		
		String hourFolder = PVConstants.hourFolders[config.getInt("HourOfDay", hour)];
		sLogger.info("\nStarting the hourly pv task of hour " + hourFolder + " ...");
		GeneratingPV gPV = new GeneratingPV();
		gPV.generatingPVHourly(config);
		sLogger.info("Ended the hourly pv task of hour " + hourFolder + ".\n");

		return 0;		
	}		
}
