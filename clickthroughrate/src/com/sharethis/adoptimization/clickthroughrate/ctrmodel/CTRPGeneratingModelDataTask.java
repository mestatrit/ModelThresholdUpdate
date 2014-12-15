package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;


public class CTRPGeneratingModelDataTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(CTRPGeneratingModelDataTask.class);
	
	public int run(String[] args) throws Exception {
		sLogger.info("Getting the configuration ...");
		Configuration config = ConfigurationUtil.setConf(args);
		sLogger.info("Getting the configuration is done.");	

		sLogger.info("Starting the task to generate the data and models ...\n");
		CTRPGeneratingModelData ga = new CTRPGeneratingModelData();
		ga.generatingModelData(config);
		sLogger.info("Generating the data and models is done.\n");

		if(config.getBoolean("CTRPUploadingFlag", false)){		
			sLogger.info("Starting the task to upload the ctr predictive models into the table ...\n");
			CTRPUploadingModels upModel = new CTRPUploadingModels();
			upModel.uploadingModels(config);
			sLogger.info("Uploading the ctr predictive models into the table is done.\n");
		}
		return 0;		
	}			
}
