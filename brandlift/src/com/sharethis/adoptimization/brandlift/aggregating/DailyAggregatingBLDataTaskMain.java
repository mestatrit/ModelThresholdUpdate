package com.sharethis.adoptimization.brandlift.aggregating;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class DailyAggregatingBLDataTaskMain
{
	private static Logger sLogger = Logger.getLogger(DailyAggregatingBLDataTaskMain.class);
		
    public static void main(String[] args) throws Exception {
		sLogger.info("Starting the task to generate the ctr ...\n");
		Configuration config = new Configuration();
        Class<?> jobClass = null;
        Tool tool = null;
        int res = 0;
        jobClass = Class.forName("com.sharethis.adoptimization.brandlift.aggregating.DailyAggregatingBLDataTask");
        tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        res = ToolRunner.run(config, tool, args);

        jobClass = Class.forName("com.sharethis.adoptimization.brandlift.aggregating.PeriodicalAggregatingBLDataTask");
        tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        res = ToolRunner.run(config, tool, args);
        
        System.exit(res);
		sLogger.info("Ended the task to generate the ctr!\n");
    }
}
