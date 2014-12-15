package com.sharethis.adoptimization.clickthroughrate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 * main class to run the computation of the CTR.
 * The command line to run: hadoop jar ClickThroughRateRunner.jar 
 * com.sharethis.adoptimization.clickthroughrate.DailyGeneratingCTREachTaskMain
 */


public class DailyGeneratingCTREachTaskMain
{
	private static Logger sLogger = Logger.getLogger(DailyGeneratingCTREachTaskMain.class);
		
    public static void main(String[] args) throws Exception {
		sLogger.info("Starting the task to generate the ctr ...\n");
		Configuration config = new Configuration();
        Class<?> jobClass = Class.forName("com.sharethis.adoptimization.clickthroughrate.DailyGeneratingCTREachTask");
        Tool tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        int res = ToolRunner.run(config, tool, args);
        //System.exit(res);
		sLogger.info("Ended the task to generate the ctr!\n");
		
		sLogger.info("Starting the task to generate the daily model data ...\n");
        jobClass = Class.forName("com.sharethis.adoptimization.clickthroughrate.ModDailyGeneratingDataTask");
        tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        res = ToolRunner.run(config, tool, args);
		sLogger.info("Ended the task to generate the daily model data!\n");
		System.exit(res);
    }
}
