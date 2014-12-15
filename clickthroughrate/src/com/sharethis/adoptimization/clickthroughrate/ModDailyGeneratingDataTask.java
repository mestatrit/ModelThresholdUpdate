package com.sharethis.adoptimization.clickthroughrate;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;


public class ModDailyGeneratingDataTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(ModDailyGeneratingDataTask.class);
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public int run(String[] args) throws Exception {
		sLogger.info("Getting the configuration ...");
		Configuration config = ConfigurationUtil.setConf(args);
		sLogger.info("Getting the configuration is done.");	
		String pDateStr = config.get("ProcessingDate");
		if(pDateStr==null){
			Date pDate = new Date();
			pDateStr = sdf.format(pDate);
			config.set("ProcessingDate", pDateStr);
		}
		ModDailyGeneratingData modData = new ModDailyGeneratingData();
		modData.dailyGeneratingData(config);		
		return 0;		
	}				
}
