package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class DailyGeneratingPredCTRTaskMain
{
	private static Logger sLogger = Logger.getLogger(DailyGeneratingPredCTRTaskMain.class);
		
    public static void main(String[] args) throws Exception {
		sLogger.info("Starting the task to generate the ctr ...\n");
		Configuration config = new Configuration();
        Class<?> jobClass = Class.forName("com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation.DailyGeneratingPredCTRTask");
        Tool tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        int res = ToolRunner.run(config, tool, args);
        System.exit(res);
		sLogger.info("Ended the task to generate the ctr!\n");
    }
}
