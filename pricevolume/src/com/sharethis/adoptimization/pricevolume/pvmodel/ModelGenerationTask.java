package com.sharethis.adoptimization.pricevolume.pvmodel;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;


public class ModelGenerationTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(ModelGenerationTask.class);
	
	public int run(String[] args) throws Exception {
		sLogger.info("Getting the configuration ...");
		Configuration config = ConfigurationUtil.setConf(args);
		sLogger.info("Getting the configuration is done.");	
		sLogger.info("Starting the task to generate the base models ...\n");
		ModelGenerationBase mb = new ModelGenerationBase();
		mb.generatingBaseModels(config);
		sLogger.info("Generating the base models is done.\n");
		sLogger.info("Starting the task to upload the base models ...\n");
		ModelEvaluationUtils modUtils = new ModelEvaluationUtils();
		ModelUploadingModel upModel = new ModelUploadingModel();
		upModel.uploadingPVModelData(config);
		modUtils.updatingBidDataIntoModelTable(config) ;
		sLogger.info("Uploading the base models is done.\n");
		return 0;		
	}			
}
