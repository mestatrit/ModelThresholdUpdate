package com.sharethis.adoptimization.clickthroughrate;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.CTRConstants;

/**
 * This is the assembly to read success bid data.
 */

public class HourlyDataAll extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataAll.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public HourlyDataAll(Map<String, Tap> sources, String allFilePath, String allFileName, 
			String pDateStr, int hour, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{		
			// Defining the fields for the all data file.
	    	Fields allFields = new Fields("winning_price", "wp_bucket", "adslot_id", "adslotvisibility", "city", 
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

	        String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);   
			String allFilePathHourly = allFilePath + dayStr + CTRConstants.hourFolders[hour] + "/" + allFileName + "/";

    		Scheme allSourceScheme = new TextDelimited(allFields, false, mDelimiter, types);
            String pipeName = "all_" + CTRConstants.hourFolders[hour];	
            Pipe allAssembly = new Pipe(pipeName);
			
           	// Building the taps from the data files
            Tap allTap = new Hfs(allSourceScheme, allFilePathHourly);
    		JobConf	jobConf = new JobConf(config);

			if(allTap.resourceExists(jobConf)){
				sLogger.info(allFilePathHourly + " exists.");
				sources.put(pipeName, allTap);
				
				setTails(allAssembly);            
			}else{
				throw new Exception("The data path: " + allFilePathHourly+ " does not exist.");
			}    		
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
