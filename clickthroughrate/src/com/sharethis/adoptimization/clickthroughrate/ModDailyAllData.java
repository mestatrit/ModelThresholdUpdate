package com.sharethis.adoptimization.clickthroughrate;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * This is the assembly to read the data.
 */

public class ModDailyAllData extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(ModDailyAllData.class);
	private static final long serialVersionUID = 1L;
		
	public ModDailyAllData(Map<String, Tap> sources, String inFilePath, String pDateStr, 
			Configuration config, String nonClickName, String clickName, String modDataName, 
			int numOfHours, int endHour, String infile_postfix) 
		throws IOException, ParseException, Exception
	{
		try{		
			ClickThroughRateUtils ctrUtils = new ClickThroughRateUtils(config);
			Map<String, List<String>> setCatMap = ctrUtils.AdSpaceCategory();
			Map<String, List<String>> crtvCatMap = ctrUtils.CreativeCategory();

	    	Fields dFields = new Fields("winning_price", "wp_bucket", "adslot_id", "adslotvisibility", "city", 
	    			"pb_google_id", "setting_id", "user_seg_list", "vertical_list", "date", "timestamp", 
	    			"campaign_name", "jid", "campaign_id", "adgroup_id", "creative_id", "domain_name", 
	    			"deal_id", "ip_address", "user_agent", "cookie", "platform_type", "st_camp_id", 
	    			"st_adg_id", "st_crtv_id", "click_flag1", "imp_flag1", "click_timestamp", 
	    			"min_bid_price","service_type","geo_target_id","mobile","model_id","model_score",
	    			"gid_age","audience_id","user_browser","user_os","user_device","crtv_size","sqi_bk","errflg","flg",
					"appId","carrierId","deviceType","isApp","isInterstitialReqauest","platform","screenOrientation",
					"deviceId","location","device_make","device_model","deviceIdType","seller_id",
					"gfid","devlat","devlon");
	    	
            Class[] types = new Class[]{double.class, int.class, String.class, String.class, String.class,
            		String.class, String.class, String.class, String.class, String.class, String.class, 
            		String.class, String.class, String.class, String.class, String.class, String.class,
            		String.class, String.class, String.class, String.class, String.class, String.class,
            		String.class, String.class, int.class, int.class, String.class,
            		long.class, String.class, String.class, String.class, long.class, double.class,
            		String.class, String.class, String.class, String.class, String.class, long.class, int.class, int.class, int.class,
            		String.class, String.class, String.class, String.class, String.class, String.class, String.class,
            		String.class, String.class, String.class, String.class, String.class, String.class,
            		String.class, String.class, String.class};    
            
            //Fields newKeyFields = new Fields("user_seg_id", "vertical_id");
            Fields newCatFields = new Fields("st_setting_cat", "st_crtv_cat");
	    	Pipe[] dataAssembly = new Pipe[2];
	    	
            dataAssembly[0] = new ModDailyNonClick(sources, inFilePath, dFields, types,
	    			pDateStr, config, nonClickName, numOfHours, endHour, infile_postfix, 
	    			setCatMap, crtvCatMap, newCatFields).getTails()[0]; 

	    	dataAssembly[1] = new ModDailyClickData(sources, inFilePath, dFields, types,
	    			pDateStr, config, clickName, numOfHours, endHour, infile_postfix, 
	    			setCatMap, crtvCatMap, newCatFields).getTails()[0]; 

	    	Fields keptFields = new Fields("winning_price", "wp_bucket", "adslot_id", "adslotvisibility", "city", 
	    			"pb_google_id", "setting_id", "user_seg_list", "vertical_list", "date", "timestamp", 
	    			"campaign_name", "jid", "campaign_id", "adgroup_id", "creative_id", "domain_name", 
	    			"deal_id", "ip_address", "user_agent", "cookie", "platform_type", "st_camp_id", 
	    			"st_adg_id", "st_crtv_id", "click_flag1", "imp_flag1", "click_timestamp", 
	    			"min_bid_price","service_type","geo_target_id","mobile","model_id","model_score",
	    			"gid_age","audience_id","user_browser","user_os","user_device","crtv_size",
	    			"sqi_bk","errflg","flg","seller_id","gfid","devlat","devlon",
	    			"st_setting_cat","st_crtv_cat");

	    	dataAssembly[0] = new Each(dataAssembly[0], keptFields, new Identity());
	    	dataAssembly[1] = new Each(dataAssembly[1], keptFields, new Identity());

			Pipe modAssembly = new Pipe(modDataName, new GroupBy(dataAssembly, keptFields));

			setTails(modAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
