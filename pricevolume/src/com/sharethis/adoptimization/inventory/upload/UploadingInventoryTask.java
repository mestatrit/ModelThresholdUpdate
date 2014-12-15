package com.sharethis.adoptimization.inventory.upload;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;
import com.sharethis.adoptimization.pricevolume.PriceVolumeUtils;

public class UploadingInventoryTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(UploadingInventoryTask.class);
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
		boolean uploadingInvFlag = config.getBoolean("UploadingInvFlag", false);
        if(uploadingInvFlag){
        	PriceVolumeUtils pvUtils = new PriceVolumeUtils(config);
        	sLogger.info("Starting uploading inventory data ...\n");
        	UploadingInventory upBid = new UploadingInventory();
        	int retVal = upBid.uploadingInventory(config);
        	sLogger.info("Ended uploading the inventory data task.\n");
        	if(retVal==0){
        		sLogger.info("Updating the admin date into the table ...");	
        		String pDateStr = config.get("ProcessingDate");
        		Date pDate = sdf.parse(pDateStr);			
        		String poolName = config.get("PoolName");
        		String keyVal = "InventoryAggregation";
        		Date adminDate = pvUtils.getAdminDate(poolName, keyVal);
        		if(adminDate==null)
        			pvUtils.upsertAdminDate(poolName, keyVal, pDate);
        		else
        			if(pDate.after(adminDate))
        				pvUtils.upsertAdminDate(poolName, keyVal, pDate);
        		sLogger.info("Updating the admin date into the table is done.");	
        	}
        }
		return 0;		
	}	
}
