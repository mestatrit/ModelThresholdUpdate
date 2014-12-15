package com.sharethis.adoptimization.campaigndata;

import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.LeftJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.FilterOutNullData;


/**
 * This is the assembly to join the bidder and price confirmation data.
 */

public class HourlyDataImpressionClickJson extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataImpressionClickJson.class);
	private static final long serialVersionUID = 1L;

	public HourlyDataImpressionClickJson(Map<String, Tap> sources, String impFilePath, String clickFilePath,
			String clickFilePathSVR, String pDateStr, int hour, int numOfHoursClick, Configuration config) 
			throws IOException, ParseException, Exception
	{
		try{	
			Pipe clickAssembly = null;
			Pipe impAssembly = null;
				
			Pipe clickAssemblyRTB = null;
			Pipe clickAssemblySVR = null;
			clickAssemblyRTB = new HourlyDataClickJson(sources, clickFilePath, pDateStr, 
						hour, numOfHoursClick, config).getTails()[0];
			
			if(clickAssemblyRTB != null){
				clickAssemblyRTB = new Unique(clickAssemblyRTB, new Fields("click_domain_name","click_ip_address","click_aid","click_cid","click_cookie"), 10);
			}

			clickAssemblySVR = new HourlyDataClickJson(sources, clickFilePathSVR, pDateStr, 
						hour, numOfHoursClick, config).getTails()[0];
			clickAssembly = new Merge("click_hourly", new Pipe[]{clickAssemblyRTB, clickAssemblySVR});
			if(clickAssembly != null)
				clickAssembly = new Unique(clickAssembly, new Fields("click_jid"), 100000);
				
			impAssembly = new HourlyDataImpressionJson(sources, impFilePath, pDateStr, hour, config).getTails()[0];

			Pipe ciAssembly = null;
			Pipe ciCount = null;
			if (!(clickAssembly==null) && !(impAssembly==null)){
			    // Defining the kept fields after the data is merged.
			    Fields keptFields = new Fields("date","timestamp","campaign_name","jid","campaign_id","adgroup_id","creative_id",
						"cth","ctw","domain_name","did","ip_address","user_agent","cookie","gogle_id","service_type",  
						"st_camp_id", "st_adg_id", "st_crtv_id", "errflg","flg","gfid","devlat","devlon",
						"click_jid","click_date","click_timestamp","log","click_flag1","imp_flag1");
							
			    // Building the pipe to join the data
			    Fields impKeyFields = new Fields("jid");
			    Fields clickKeyFields = new Fields("click_jid");
//			    Fields impKeyFields = new Fields("jid","service_type");
//			    Fields clickKeyFields = new Fields("click_jid","click_service_type");
			    //Joining impAssembly and clickAssembly
			    ciAssembly = new CoGroup("imp_click", impAssembly, impKeyFields, clickAssembly, clickKeyFields, new LeftJoin());
			    Function<?> clickFunc = new AssignClickImpInt(new Fields("click_flag1", "imp_flag1"));
			    ciAssembly = new Each(ciAssembly, new Fields("click_jid", "jid"), clickFunc, Fields.ALL);
				Filter<?> filterOutNull = new FilterOutNullData();
				ciAssembly = new Each(ciAssembly, new Fields("jid"), filterOutNull); 
			    //Keeping the fields defined in keptFields.
			    ciAssembly = new Each(ciAssembly, keptFields, new Identity());

				SumBy sum1 = new SumBy(new Fields("imp_flag1"), new Fields("sum_imp"), int.class); 
				SumBy sum2 = new SumBy(new Fields("click_flag1"), new Fields("sum_click"), int.class); 
				ciCount = new AggregateBy("imp_click_count", new Pipe[]{ciAssembly}, new Fields("adgroup_id"), sum1, sum2); 
			}
			setTails(ciAssembly, ciCount);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}	
}
