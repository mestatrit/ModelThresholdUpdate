package com.sharethis.adoptimization.inventory;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 * main class to run the computation of the daily no bid data.
 */


public class DailyInventoryMain
{
	private static Logger sLogger = Logger.getLogger(DailyInventoryMain.class);
		
    public static void main(String[] args) throws Exception {
		sLogger.info("Starting the task to generate the daily nobid data ...\n");
		Configuration config = new Configuration();
        Class<?> jobClass = Class.forName("com.sharethis.adoptimization.inventory.DailyInventoryTask");
        Tool tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        int res = ToolRunner.run(config, tool, args);
        System.exit(res);
		sLogger.info("Ended the task to generate the nobid data!\n");
    }
}
