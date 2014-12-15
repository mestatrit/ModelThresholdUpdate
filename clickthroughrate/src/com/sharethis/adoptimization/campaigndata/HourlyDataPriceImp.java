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

public class HourlyDataPriceImp extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataPriceImp.class);
	private static final long serialVersionUID = 1L;

	public HourlyDataPriceImp(Map<String, Tap> sources, String impFilePath,
			String pDateStr, int hour, String priceFilePath, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{				
			Pipe[] priceAssembly = new HourlyDataPrice(sources, priceFilePath, pDateStr, hour, config).getTails();

			Pipe impAssembly = new HourlyDataImpressionJson(sources, impFilePath, pDateStr, hour, config).getTails()[0];		    

		    Pipe piAssembly = null;
		    		
		    if (!(priceAssembly==null) && !(impAssembly==null)){
	            // Defining the kept fields.
		    	Fields keptFields = new Fields("date0","timestamp1","campaign_name","jid1","campaign_id","adgroup_id1","creative_id1",
					"cth","ctw","domain_name","did","ip_address","user_agent","cookie","gogle_id","platform_type1",  
					"st_camp_id", "st_adg_id", "st_crtv_id","winning_price","errflg","flg","gfid","devlat","devlon","wp_bucket");

		    	// Building the pipe to join the data
		    	Fields priceKeyFields = new Fields("jid1");
		    	Fields impKeyFields = new Fields("jid");
//		    	Fields impKeyFields = new Fields("jid","service_type");
//		    	Fields priceKeyFields = new Fields("jid1","platform_type");
		    	//Joining impAssembly and priceAssembly
				piAssembly = new CoGroup("pi_data", priceAssembly[0], priceKeyFields, impAssembly, impKeyFields, new LeftJoin());
				piAssembly = new Each(piAssembly, keptFields, new Identity());
		    }
		    setTails(piAssembly, priceAssembly[1], priceAssembly[2]);	
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}		
}
