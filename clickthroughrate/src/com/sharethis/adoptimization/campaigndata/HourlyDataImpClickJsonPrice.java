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
 * This is the assembly to join the bidder and price confirmation data.
 */

public class HourlyDataImpClickJsonPrice extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataImpClickJsonPrice.class);
	private static final long serialVersionUID = 1L;

	public HourlyDataImpClickJsonPrice(Map<String, Tap> sources, String impFilePath, String clickFilePath, String clickFilePathSVR,
			String pDateStr, int hour, int numOfHoursClick, String priceFilePath, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{				

		    Pipe[] icAssembly = new HourlyDataImpressionClickJson(sources, impFilePath, clickFilePath, 
		    		clickFilePathSVR, pDateStr, hour, numOfHoursClick, config).getTails();	
		    
			Pipe[] priceAssembly = new HourlyDataPriceMultiHours(sources, priceFilePath, pDateStr, hour, config).getTails();

		    Pipe icpAssembly = null;
		    		
		    if (!(priceAssembly==null) && !(icAssembly==null)){
	            // Defining the kept fields.
		    	Fields keptFields = new Fields("date","timestamp","campaign_name","jid","campaign_id","adgroup_id","creative_id",
					"cth","ctw","domain_name","did","ip_address","user_agent","cookie","gogle_id","service_type",  
					"st_camp_id", "st_adg_id", "st_crtv_id","click_jid","click_date","click_timestamp",
					"click_flag1","imp_flag1","winning_price","errflg","flg","gfid","devlat","devlon","log","wp_bucket");
		    	
		    	// Building the pipe to join the data
		    	Fields icKeyFields = new Fields("jid");
		    	Fields priceKeyFields = new Fields("jid1");
//		    	Fields icKeyFields = new Fields("jid","service_type");
//		    	Fields priceKeyFields = new Fields("jid1","platform_type");
		    	//Joining icAssembly and priceAssembly
				icpAssembly = new CoGroup("icp_data", icAssembly[0], icKeyFields, priceAssembly[0], priceKeyFields, new LeftJoin());
				icpAssembly = new Each(icpAssembly, keptFields, new Identity());
//				icpAssembly = new Unique(icpAssembly, new Fields("adgroup_id","domain_name","ip_address","gogle_id"), 100000);
		    }
		    setTails(icpAssembly, icAssembly[1], priceAssembly[1], priceAssembly[2]);	
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}		
}
