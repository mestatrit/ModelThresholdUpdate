package com.sharethis.adoptimization.inventory;

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

public class HourlyInventoryTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(HourlyInventoryTask.class);
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
		
		HourlyInventoryGeneration gNB = new HourlyInventoryGeneration();
		String sHourStr = config.get("StartHour");
		String eHourStr = config.get("EndHour");
		if(sHourStr!=null && eHourStr!=null){
			int startHour = Integer.parseInt(sHourStr);
			int endHour = Integer.parseInt(eHourStr);
			for(int i=startHour; i<endHour; i++){
				config.setInt("HourOfDay", i);
				String hourFolder = PVConstants.hourFolders[config.getInt("HourOfDay", i)];
				sLogger.info("\nStarting the hourly inventory task of hour " + hourFolder + " ...");
				gNB.generatingInventoryHourly(config);
				sLogger.info("Ended the hourly inventory task of hour " + hourFolder + ".\n");
			}		
		}else{
			String hourFolder = PVConstants.hourFolders[config.getInt("HourOfDay", hour)];
			sLogger.info("\nStarting the hourly inventory data task of hour " + hourFolder + " ...");
			gNB.generatingInventoryHourly(config);
			sLogger.info("Ended the hourly inventory data task of hour " + hourFolder + ".\n");
		}
		return 0;		
	}		
}
