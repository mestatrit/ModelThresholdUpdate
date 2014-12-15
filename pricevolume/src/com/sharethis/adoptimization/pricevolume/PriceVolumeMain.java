package com.sharethis.adoptimization.pricevolume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * main class to run the computation of the price-volume.
 * The command line to run: hadoop jar PriceOptRunner.jar com.sharethis.priceopt.pricevolume.GeneratingPVTaskMain
 */

public class PriceVolumeMain
{
	private static Logger sLogger = Logger.getLogger(PriceVolumeMain.class);
		
    public static void main(String[] args) throws Exception {
		sLogger.info("Starting the task to generate the pv ...\n");
		Configuration config = new Configuration();
        Class<?> jobClass1 = Class.forName("com.sharethis.adoptimization.pricevolume.GeneratingPVTask");
        Tool tool1 = (Tool) jobClass1.newInstance();
        sLogger.info("Found class name : " + jobClass1.getName());
        int res1 = ToolRunner.run(config, tool1, args);
	   	sLogger.info("Ended the task to generate the pv!\n");
		
		sLogger.info("Starting uploading price volume data ...\n");
        Class<?> jobClass2 = Class.forName("com.sharethis.adoptimization.pricevolume.upload.UploadingPVDataTask");
        Tool tool2 = (Tool) jobClass2.newInstance();
        sLogger.info("Found class name : " + jobClass2.getName());
        int res2 = ToolRunner.run(config, tool2, args);
		sLogger.info("Ended loading the price volume data!\n");				
        System.exit(res2);
    }
}
