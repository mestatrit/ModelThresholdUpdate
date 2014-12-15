package com.sharethis.adoptimization.pricevolume;

import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.filter.Not;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Unique;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.FilterOutDataEQStr;
import com.sharethis.adoptimization.common.FilterOutDataNEStrings;
import com.sharethis.adoptimization.common.FilterOutNullData;
import com.sharethis.adoptimization.common.PVConstants;


/**
 * This is the assembly to read bidder data.
 */

public class HourlyDataBidder extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataBidder.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
	
	public HourlyDataBidder(Map<String, Tap> sources, String bidFilePath, String pDateStr, int hour, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{				
			// Defining the fields of bidder data

			Fields bidFields = new Fields("timestamp","eventType","adslotid","adslotvisibility",
					"campaign_id","crtv_height","crtv_width","adgroup_id","creative_id",
					"bid_price","result_reason","city","country","google_id","ip_address",
					"model_id","reference_id","network","sharethis_id","domain_name","url","user_browser",
					"user_os","user_device","jid","timestamp_r","deal_id","setting_id","user_list",
					"vertical_list","min_bid_price");
            Class[] types = new Class[]{String.class, String.class, String.class, String.class, 
            		String.class,int.class,int.class,String.class,String.class,
            		double.class, String.class, String.class, String.class,String.class,String.class,
            		int.class,int.class,String.class,String.class,String.class,String.class,String.class,
            		String.class,String.class,String.class,String.class,String.class,String.class,String.class,
            		String.class,long.class};           
           
            Fields keptFields = new Fields("timestamp","adslotid","adslotvisibility","campaign_id","adgroup_id",
            		"creative_id","bid_price","result_reason","city","google_id","ip_address","url",
					"domain_name","user_browser","user_os","user_device","jid","deal_id","setting_id","user_list", 
					"vertical_list","min_bid_price");
            
            String hourFolder = PVConstants.hourFolders[hour]; 
            String pipeName = "bidSource_" + hourFolder;
            Pipe bidAssembly = new Pipe(pipeName);
			// Building the taps from the data files
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);            
            String bidFilePathHourly = bidFilePath + dayStr + "/" + hourFolder;
			Scheme bidSourceScheme = new TextDelimited(bidFields, false, mDelimiter, types);
			//ZipFileInputFormat.setLenient(true);
			Tap bidTap = new Hfs(bidSourceScheme, bidFilePathHourly);
			JobConf	jobConf = new JobConf(config);
			if(bidTap.resourceExists(jobConf)){
				sLogger.info(bidFilePathHourly + " exists.");
				sources.put(pipeName, bidTap);

				// Drop useless data fields/columns
				bidAssembly = new Each(bidAssembly, keptFields, new Identity());
				Pipe sPipe = new Pipe("success_bid", bidAssembly); 
				Pipe nsPipe = null;

				if(config.getBoolean("NobidFlag", true)){
					// Filtering out the data
					Filter<?> filterOutSuccess = new FilterOutDataNEStrings("Success");
					sPipe = new Each(sPipe, new Fields("result_reason"), filterOutSuccess); 
					sPipe = new Unique(sPipe, new Fields("jid"));
					sLogger.info("sPipeName: " + sPipe.getName());
					if(config.getBoolean("InvFlag", false)){
						nsPipe = new Pipe("non_success_bid", bidAssembly); 
						nsPipe = new Each(nsPipe, new Fields("result_reason"), new Not(filterOutSuccess)); 
						sLogger.info("nsPipeName: " + nsPipe.getName());
						nsPipe = new Unique(nsPipe, new Fields("jid"));
					}
				}
				setTails(sPipe, nsPipe);
			}else{
				throw new Exception("The data path: " + bidFilePathHourly+ " does not exist.");				
			}
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
	
	public HourlyDataBidder(Map<String, Tap> sources, String bidFilePath, String pDateStr, int hour, Configuration config, int dataSource) 
			throws IOException, ParseException, Exception
	{
		try{				
			// Defining the fields of bidder data
			Fields bidFields = new Fields("date","ad","adslotid","adslotvisibility",
					"campaign_id","crtv_height","crtv_width","adgroup_id","creative_id",
					"bid_price","result_reason","city","country","google_id","ip_address",
					"model_list","reference_id","seller_id","sharethis_id","url","user_agent",
					"jid","timestamp","deal_id","setting_id","user_list",
					"vertical_list","min_bid_price","platform_type","geo_target_id","mobile",
					"model_score_old","gid_age","audience_id","req_adgroup_id","sqi","x_flag","view_str");
/*	        Class[] types = new Class[]{String.class, String.class, String.class, String.class, 
	            	String.class,int.class,int.class,String.class,String.class,
	            	double.class, String.class, String.class, String.class,String.class,String.class,
	            	String.class,int.class,String.class,String.class,String.class,String.class,
	            	String.class,String.class,String.class,String.class,String.class,
	            	String.class,long.class,String.class,String.class,String.class,
	            	String.class,long.class,String.class,String.class,String.class,int.class,String.class};           
*/
	        Class[] types = new Class[]{String.class, String.class, String.class, String.class, 
	            	String.class,String.class,String.class,String.class,String.class,
	            	String.class,String.class,String.class,String.class,String.class,String.class,
	            	String.class,String.class,String.class,String.class,String.class,String.class,
	            	String.class,String.class,String.class,String.class,String.class,
	            	String.class,String.class,String.class,String.class,String.class,
	            	String.class,String.class,String.class,String.class,String.class,String.class,String.class};           
	        
	        Fields keptFields = new Fields("timestamp","adslotid","adslotvisibility","campaign_id","adgroup_id",
	            	"creative_id","bid_price","result_reason","city","google_id","ip_address",
					"domain_name","user_browser","user_os","user_device","jid","deal_id",
					"setting_id","user_list","vertical_list","min_bid_price","platform_type",
					"geo_target_id","mobile","model_id","model_score","gid_age","audience_id","crtv_size","sqi_bk", 
					"appId","carrierId","deviceType","isApp","isInterstitialReqauest","platform","screenOrientation",
					"deviceId","location","device_make","device_model","deviceIdType","seller_id");
	            	          
	        String hourFolder = PVConstants.hourFolders[hour]; 	        
	        String pipeName = "bidSource_" + hourFolder;    
	        Pipe bidAssembly = new Pipe(pipeName);
				
	        // Building the taps from the data files
	        String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);   
//	        String bidFilePathHourly = bidFilePath + dayStr + "/" + hourFolder;
            // The change is to handle the new data path: yyyyMMddhh
	        String bidFilePathHourly = bidFilePath + dayStr + hourFolder;

			Scheme bidSourceScheme = new TextDelimited(bidFields, null, false, false, mDelimiter, false, "", types, true);

			Tap bidTap = new Hfs(bidSourceScheme, bidFilePathHourly);
			JobConf	jobConf = new JobConf(config);
			if(bidTap.resourceExists(jobConf)){
				sLogger.info(bidFilePathHourly + " exists.");
				sources.put(pipeName, bidTap);

				Function<?> parser = new ParsingDataFields(new Fields("domain_name", "user_browser", "user_os", 
						"user_device", "crtv_size", "sqi_bk", "model_id","model_score",
						"appId","carrierId","deviceType","isApp","isInterstitialReqauest","platform","screenOrientation",
						"deviceId","location","device_make","device_model","deviceIdType"));
				
				bidAssembly = new Each(bidAssembly, new Fields("url","user_agent","crtv_height","crtv_width","sqi","model_list","mobile"), parser, Fields.ALL);			

				// Drop useless data fields/columns
				bidAssembly = new Each(bidAssembly, keptFields, new Identity());
				Pipe sPipe = new Pipe("success_bid", bidAssembly); 
				//**************************************************************************
				Filter<?> filterOutAdGroup = new FilterOutDataEQStr("6584");
				sPipe = new Each(sPipe, new Fields("adgroup_id"), filterOutAdGroup); 
				//**************************************************************************
				Filter<?> filterOutNull = new FilterOutNullData();
				sPipe = new Each(sPipe, new Fields("jid"), filterOutNull); 
				sPipe = new Unique(sPipe, new Fields("jid"), 100000);

				Pipe nsPipe = null;

				if(config.getBoolean("NobidFlag", true)){
					// Filtering out the data
					Filter<?> filterOutSuccess = new FilterOutDataNEStrings("Success");
					sPipe = new Each(sPipe, new Fields("result_reason"), filterOutSuccess); 

					sLogger.info("sPipeName: " + sPipe.getName());
					if(config.getBoolean("InvFlag", false)){
						nsPipe = new Pipe("non_success_bid", bidAssembly); 
						nsPipe = new Each(nsPipe, new Fields("result_reason"), new Not(filterOutSuccess)); 
						sLogger.info("nsPipeName: " + nsPipe.getName());
					}
				}
				setTails(sPipe, nsPipe);
			}else{
				throw new Exception("The data path: " + bidFilePathHourly+ " does not exist.");				
			}
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}		
}
