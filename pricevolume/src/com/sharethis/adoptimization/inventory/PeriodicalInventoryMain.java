package com.sharethis.adoptimization.inventory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 * main class to run the computation of the no bid data.
 */


public class PeriodicalInventoryMain
{
	private static Logger sLogger = Logger.getLogger(PeriodicalInventoryMain.class);
		
    public static void main(String[] args) throws Exception {
		sLogger.info("Starting the task to generate the inventory data ...\n");
		Configuration config = new Configuration();
        Class<?> jobClass = Class.forName("com.sharethis.adoptimization.inventory.PeriodicalInventoryTask");
        Tool tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        int res = ToolRunner.run(config, tool, args);
		sLogger.info("Ended the task to generate the inventory data!\n");
		sLogger.info("Starting uploading inventory data ...\n");
        jobClass = Class.forName("com.sharethis.adoptimization.inventory.upload.UploadingInventoryTask");
        tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        res = ToolRunner.run(config, tool, args);
        System.exit(res);
		sLogger.info("Ended loading the inventory data!\n");		
    }
}
