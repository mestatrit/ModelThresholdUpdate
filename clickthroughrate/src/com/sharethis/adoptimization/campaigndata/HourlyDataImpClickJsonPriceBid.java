package com.sharethis.adoptimization.campaigndata;

import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.joiner.LeftJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * This is the assembly to join the imp-click-price and successful bid data.
 */

public class HourlyDataImpClickJsonPriceBid extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataImpClickJsonPriceBid.class);
	private static final long serialVersionUID = 1L;

	public HourlyDataImpClickJsonPriceBid(Map<String, Tap> sources, String impFilePath, String clickFilePath,
			String clickFilePathSVR, String pDateStr, int hour, int numOfHoursClick, String priceFilePath, 
			String sbFilePath, String sbFileName, int numOfHoursBid,  
			Configuration config) throws IOException, ParseException, Exception
	{
		try{				
		    Pipe[] icpAssembly = new HourlyDataImpClickJsonPrice(sources, impFilePath, clickFilePath, 
		    		clickFilePathSVR, pDateStr, hour, numOfHoursClick, priceFilePath, config).getTails();	
		    Pipe sbAssembly = new HourlyDataSuccessBid(sources, sbFilePath, sbFileName, pDateStr, 
					hour, numOfHoursBid, config).getTails()[0];

		    Pipe allAssembly = null;
		    Pipe[] adxAssembly = null;
		    
		    if (!(sbAssembly==null) && !(icpAssembly==null)){
	            // Defining the kept fields.
		    	Fields keptFields = new Fields("winning_price", "wp_bucket", "adslot_id", "adslotvisibility", "city", 
		    			"sb_google_id", "setting_id", "user_list", "vertical_list", "date", "timestamp", 
		    			"campaign_name", "jid", "campaign_id", "adgroup_id", "creative_id", "domain_name", 
		    			"deal_id", "ip_address", "user_agent", "cookie", "service_type", "st_camp_id", 
		    			"st_adg_id", "st_crtv_id", "click_flag1", "imp_flag1", "click_timestamp", 
		    			"min_bid_price","platform_type","geo_target_id","mobile","model_id","model_score",
		    			"gid_age","audience_id","user_browser","user_os","user_device","crtv_size","sqi_bk","errflg","flg",
						"appId","carrierId","deviceType","isApp","isInterstitialReqauest","platform","screenOrientation",
						"deviceId","location","device_make","device_model","deviceIdType","seller_id");
		    	
		    	// Building the pipe to join the data
		    	Fields sbKeyFields = new Fields("sb_jid");
		    	Fields icKeyFields = new Fields("jid");
//		    	Fields pbKeyFields = new Fields("pb_jid","platform_type");
//		    	Fields icKeyFields = new Fields("jid","service_type");
		    	//Joining icAssembly and sbAssembly
				allAssembly = new CoGroup("all_data", icpAssembly[0], icKeyFields, sbAssembly, sbKeyFields, new LeftJoin());
				
				//Keeping the fields defined in keptFields.
				allAssembly = new Each(allAssembly, keptFields, new Identity());
				
				adxAssembly = new HourlyDataImpClickJsonPriceBidRandom(allAssembly, keptFields, config).getTails(); 
		    }				
					    
		    setTails(icpAssembly[0], icpAssembly[1], icpAssembly[2], icpAssembly[3], allAssembly, 
		    		adxAssembly[0], adxAssembly[1], adxAssembly[2], adxAssembly[3]);		    	
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}		
}
