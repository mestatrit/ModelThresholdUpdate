package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class CTRPGeneratingDataMain
{
	private static Logger sLogger = Logger.getLogger(CTRPGeneratingDataMain.class);
/*		
    public static void main(String[] args) throws Exception {
		sLogger.info("Starting the task to generate the data for models ...\n");
		Configuration config = new Configuration();
        Class<?> jobClass = Class.forName("com.sharethis.adoptimization.clickthroughrate.ctrmodel.CTRPGeneratingDataTaskNew");
        Tool tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        int res = ToolRunner.run(config, tool, args);
        System.exit(res);
		sLogger.info("Ended the task to generate the data for models!\n");
    }
*/
	
    public static void main(String[] args) throws Exception {
        Class<?> jobClass = null;
        Tool tool = null;
        int res = 0;
		Configuration config = new Configuration();
        /*
		sLogger.info("Starting the task to generate the base data ...\n");
        jobClass = Class.forName("com.sharethis.adoptimization.clickthroughrate.ctrmodel.CTRPGeneratingBaseDataTask");
        tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        res = ToolRunner.run(config, tool, args);
		sLogger.info("Ended the task to generate the base data!\n");
		*/
		sLogger.info("Starting the task to generate the model data and models ...\n");
        jobClass = Class.forName("com.sharethis.adoptimization.clickthroughrate.ctrmodel.CTRPGeneratingModelDataTask");
        tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        res = ToolRunner.run(config, tool, args);
		sLogger.info("Ended the task to generate the mode data and models!\n");
        System.exit(res);
    }
}
