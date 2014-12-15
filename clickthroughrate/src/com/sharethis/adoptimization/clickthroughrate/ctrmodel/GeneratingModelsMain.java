package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class GeneratingModelsMain
{
	private static Logger sLogger = Logger.getLogger(GeneratingModelsMain.class);
		
    public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		Class<?> jobClass = null;
		Tool tool = null;
		int res = 0;
	
		sLogger.info("Starting the task to generate the model data and models ...\n");
        jobClass = Class.forName("com.sharethis.adoptimization.clickthroughrate.ctrmodel.CTRPGeneratingModelDataTask");
        tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        res = ToolRunner.run(config, tool, args);
		sLogger.info("Ended the task to generate the mode data and models!\n");

		sLogger.info("Starting the task to generate the PC models ...\n");
		config = new Configuration();
        jobClass = Class.forName("com.sharethis.adoptimization.clickthroughrate.ctrmodel.PCGeneratingModelTask");
        tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        res = ToolRunner.run(config, tool, args);
		sLogger.info("Generating the PC models is done.\n");

        System.exit(res);
    }
}
