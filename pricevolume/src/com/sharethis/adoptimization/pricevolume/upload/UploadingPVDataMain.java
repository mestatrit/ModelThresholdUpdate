package com.sharethis.adoptimization.pricevolume.upload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * This is the class to run the flow.
 */

public class UploadingPVDataMain
{	
	private static final Logger sLogger = Logger.getLogger(UploadingPVDataMain.class);

    public static void main(String[] args) throws Exception {
		sLogger.info("Starting uploading price volume data ...\n");
        Configuration config = new Configuration();
        Class jobClass = Class.forName("com.sharethis.adoptimization.pricevolume.upload.UploadingPVDataTask");
        Tool tool = (Tool) jobClass.newInstance();
        sLogger.info("Found class name : " + jobClass.getName());
        int res = ToolRunner.run(config, tool, args);
        System.exit(res);
		sLogger.info("Ended loading the price volume data!\n");		
    }
}
