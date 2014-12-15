package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;

public class PCGeneratingModelTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(PCGeneratingModelTask.class);
	
	public int run(String[] args) throws Exception {
		Calendar c = Calendar.getInstance();
		Date date = new Date();
		c.setTime(date);
		int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
		sLogger.info("Today is week day " + dayOfWeek);
		if(dayOfWeek==1){
			sLogger.info("Getting the configuration ...");
			Configuration config = ConfigurationUtil.setConf(args);
			sLogger.info("Getting the configuration is done.");	

			sLogger.info("Starting the task to generate the PC models ...\n");
			PCGeneratingModel pcMod = new PCGeneratingModel();
			pcMod.generatingPCModels(config);
			sLogger.info("Generating the PC models is done.\n");
		}else{
			sLogger.info("No PC models are generated!\n");				
		}
		return 0;	
	}			
}
