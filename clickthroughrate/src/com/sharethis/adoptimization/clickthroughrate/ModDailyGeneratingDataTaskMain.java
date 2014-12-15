package com.sharethis.adoptimization.clickthroughrate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 */


public class ModDailyGeneratingDataTaskMain
{
	private static Logger sLogger = Logger.getLogger(ModDailyGeneratingDataTaskMain.class);
		
    public static void main(String[] args) throws Exception {
		sLogger.info("Starting the task to generate the daily model data ...\n");
		Configuration config = new Configuration();
        Class<?> jobClass = Class.forName("com.sharethis.adoptimization.clickthroughrate.ModDailyGeneratingDataTask");
        Tool tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        int res = ToolRunner.run(config, tool, args);
        System.exit(res);
		sLogger.info("Ended the task to generate the daily model data!\n");
    }
}
