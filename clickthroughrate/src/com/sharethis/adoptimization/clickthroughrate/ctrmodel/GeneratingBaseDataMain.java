package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class GeneratingBaseDataMain
{
	private static Logger sLogger = Logger.getLogger(GeneratingBaseDataMain.class);
		
    public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		Class<?> jobClass = null;
		Tool tool = null;
		int res = 0;
		sLogger.info("Starting the task to generate the base data ...\n");
        jobClass = Class.forName("com.sharethis.adoptimization.clickthroughrate.ctrmodel.CTRPGeneratingBaseDataTask");
        tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        res = ToolRunner.run(config, tool, args);
		sLogger.info("Ended the task to generate the base data!\n");
        System.exit(res);
    }
}
