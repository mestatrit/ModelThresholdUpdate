package com.sharethis.adoptimization.pricevolume;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.sharethis.adoptimization.common.ConfigurationUtil;
import com.sharethis.adoptimization.common.PVConstants;
import com.sharethis.adoptimization.pricevolume.GeneratingPV;


public class HourlyGeneratingPVTest extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(HourlyGeneratingPVTest.class);
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public int run(String[] args) throws Exception {
		sLogger.info("Getting the configuration ...");
		Configuration config = ConfigurationUtil.setConf(args);
		sLogger.info("Getting the configuration is done.");	
		int offset = 0;
		
		if(config.get("ProcessingDate")==null){
			// If ProcessDate is null, then using the date of yesterday as the processing date.
			Date date = DateUtils.addDays(new Date(), -offset);
			config.set("ProcessingDate", sdf.format(date));
		}
		
		int startHour = 0;
		int endHour = 24;
		startHour = config.getInt("StartHour", startHour);
		endHour = config.getInt("EndHour", endHour);
		for(int i=startHour; i<endHour; i++){
			config.setInt("HourOfDay", i);
			String hourFolder = PVConstants.hourFolders[config.getInt("HourOfDay", i)];
			sLogger.info("\nStarting the hourly pv task of hour " + hourFolder + " ...");
			GeneratingPV gPV = new GeneratingPV();
			gPV.generatingPVHourly(config);
			sLogger.info("Ended the hourly pv task of hour " + hourFolder + ".\n");
		}
		return 0;		
	}			
}
