package com.sharethis.adoptimization.pricevolume.upload;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;
import com.sharethis.adoptimization.pricevolume.PriceVolumeUtils;
import com.sharethis.adoptimization.pricevolume.pvmodel.ModelEvaluationUtils;
import com.sharethis.adoptimization.pricevolume.pvmodel.ModelUploadingModel;

public class UploadingPVDataTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(UploadingPVDataTask.class);
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public int run(String[] args) throws Exception {
		sLogger.info("Getting the configuration ...");
		Configuration config = ConfigurationUtil.setConf(args);
		sLogger.info("Getting the configuration is done.");	
		if(config.get("ProcessingDate")==null){
			// If ProcessDate is null, then using the date of yesterday as the processing date.
			Date date = DateUtils.addDays(new Date(), -1);
			config.set("ProcessingDate", sdf.format(date));
		}
		if(config.getBoolean("UploadingPVFlag", false))
			uploadingData(config);
		return 0;		
	}	

	private void uploadingData(Configuration config) throws Exception {
		PriceVolumeUtils pvUtils = new PriceVolumeUtils(config);
		sLogger.info("Starting uploading price volume data ...\n");
		UploadingPVWinData upWin = new UploadingPVWinData();
		upWin.uploadingPVWinData(config);
		UploadingPVBidData upBid = new UploadingPVBidData();
		upBid.uploadingPVBidData(config);
		sLogger.info("Ended uploading the price volume data task.\n");
		sLogger.info("Starting updating inventory data ...\n");
		String pDateStr = config.get("ProcessingDate");
		String poolName = config.get("PoolName");
		String keyVal = "InventoryAggregation";
		Date adminDate = pvUtils.getAdminDate(poolName, keyVal);
		Date pDate = sdf.parse(pDateStr);			
		int invSource = 1;
		String invSourceStr = config.get("InvSource");
		if(invSourceStr==null){
			if((adminDate!=null)&&DateUtils.isSameDay(adminDate,pDate))
				invSource=0;
		}else{
			invSource = Integer.parseInt(invSourceStr);
		}
		sLogger.info("Ended updating the data into the tables.");		
		pvUtils.updatingInvDataIntoBidTable(config, invSource);
		pvUtils.updatingWinDataIntoBidTable(config);
		sLogger.info("Ended updating the inventory data task.\n");
		sLogger.info("Starting the task to upload the models ...");
		ModelUploadingModel upModel = new ModelUploadingModel();
		upModel.uploadingPVModelData(config);
		ModelEvaluationUtils modUtils = new ModelEvaluationUtils();
		modUtils.updatingBidDataIntoModelTable(config) ;
		sLogger.info("Uploading the models is done.\n");
		sLogger.info("Starting to update the data into the tables ...");
		pvUtils.insertingPriceVolumeCurve(config, invSource);
		keyVal = "PriceVolume";
		adminDate = pvUtils.getAdminDate(poolName, keyVal);
		if(pDate.after(adminDate))
			pvUtils.upsertAdminDate(poolName, keyVal, pDate);
		sLogger.info("Ended updating the data into the tables.");				
	}			
}
