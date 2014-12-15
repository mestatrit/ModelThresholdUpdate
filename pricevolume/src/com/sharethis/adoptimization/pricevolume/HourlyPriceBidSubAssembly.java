package com.sharethis.adoptimization.pricevolume;

import cascading.operation.Function;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Rename;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * This is the assembly to join the bidder and price confirmation data.
 */

public class HourlyPriceBidSubAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyPriceBidSubAssembly.class);
	private static final long serialVersionUID = 1L;
	
	public HourlyPriceBidSubAssembly(Map<String, Tap> sources, String bidFilePath, String priceFilePath,
			String pDateStr, int hour, String hourFolder, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{		

			// Defining the kept fields after bidder and price are merged.
			Fields keptFields = new Fields("timestamp1","jid1","adgroup_id1","creative_id1","winning_price","wp_bucket",
					"jid","timestamp","adslotid","adslotvisibility","campaign_id","result_reason","bid_price","city",
					"google_id","ip_address","domain_name","user_browser","user_os","user_device","deal_id",
					"setting_id","user_seg_list","user_seg_id","vertical_list","min_bid_price","platform_type",
					"geo_target_id","mobile","model_id","model_score","gid_age","audience_id","crtv_size","sqi_bk",
					"appId","carrierId","deviceType","isApp","isInterstitialReqauest","platform","screenOrientation",
					"deviceId","location","device_make","device_model","deviceIdType","seller_id");
			
			Pipe priceAssembly = new HourlyDataPriceconf(sources, priceFilePath, pDateStr, hour, config).getTails()[0];
			Pipe[] bidAssembly = null;
			
			int dataSource = config.getInt("DataSource", 0);
//			if(dataSource==0)
//				bidAssembly = new HourlyDataBidder(sources, bidFilePath, pDateStr, hour, config).getTails();
//			else
				bidAssembly = new HourlyDataBidder(sources, bidFilePath, pDateStr, hour, config, dataSource).getTails();				
			
			// Building the pipe to join the bid and price data
			Fields priceKeyFields = new Fields("jid1","platform_type1");
			Fields bidKeyFields = new Fields("jid","platform_type");
			Pipe pbAssembly = new Pipe("priceconf-bid");
			//Joining bidAssembly and priceAssembly
			pbAssembly = new CoGroup("priceconf-bid", bidAssembly[0], bidKeyFields, priceAssembly, priceKeyFields, new InnerJoin());
//			pbAssembly = new CoGroup("priceconf_bid", priceAssembly, priceKeyFields, bidAssembly[0], bidKeyFields, new LeftJoin());
			//Keeping the fields defined in keptFields.
//			PriceVolumeUtils pvUtils = new PriceVolumeUtils(config);
//			Map<String, List<String>> adgroupUserMap = pvUtils.adgroupUserMapping();
//			if(adgroupUserMap!=null)
//				sLogger.info("adgroup user mapping's size: " + adgroupUserMap.size());
//			else
//				sLogger.info("adgroup user mapping is null.");
				
			Function<?> assignUser = new AssignUserIdNew(new Fields("user_seg_list","user_seg_id"));
			pbAssembly = new Each(pbAssembly, new Fields("user_list"), assignUser, keptFields);	
			//pbAssembly = new Each(pbAssembly, keptFields, new Identity());
			pbAssembly = new Rename(pbAssembly, new Fields("adgroup_id1","creative_id1"),
					new Fields("adgroup_id","creative_id"));
			setTails(pbAssembly, bidAssembly[0], bidAssembly[1]);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}	
}
