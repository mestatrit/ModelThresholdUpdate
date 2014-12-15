package com.sharethis.adoptimization.campaigndata;

import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
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

public class HourlyDataPriceImpClick extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataPriceImpClick.class);
	private static final long serialVersionUID = 1L;

	public HourlyDataPriceImpClick(Map<String, Tap> sources, String impFilePath, String clickFilePath,
			String clickFilePathSVR, String priceFilePath, String pDateStr, int hour, int numOfHoursClick, 
			Configuration config) throws IOException, ParseException, Exception
	{
		try{	
			Pipe[] piAssembly = new HourlyDataPriceImp(sources, impFilePath, pDateStr, hour, priceFilePath, config).getTails();

			Pipe clickAssembly = null;

			Pipe clickAssemblyRTB = null;
			Pipe clickAssemblySVR = null;
			clickAssemblyRTB = new HourlyDataClickJson(sources, clickFilePath, pDateStr, 
						hour, numOfHoursClick, config).getTails()[0];
			
			if(clickAssemblyRTB != null){
//				clickAssemblyRTB = new Unique(clickAssemblyRTB, new Fields("click_domain_name","click_ip_address",
//						"click_aid", "click_cid","click_cookie"), 10);

				if(config.getInt("ClickDupFlag", 0)==1){
					//Filtering out the duplicates of clicks based on the following fields:
					//"click_aid","click_cid","click_ip_address","click_domain_name" 
					//in the certain time frame based on "click_timestamp"
					Fields groupFields = new Fields("click_aid","click_cid","click_ip_address","click_domain_name");
					Fields sortFields = new Fields("click_timestamp");
					clickAssemblyRTB = new GroupBy(clickAssemblyRTB, groupFields, sortFields);
					clickAssemblyRTB = new Unique(clickAssemblyRTB, groupFields, 50);
				}
			}
			
			clickAssemblySVR = new HourlyDataClickJson(sources, clickFilePathSVR, pDateStr, 
						hour, numOfHoursClick, config).getTails()[0];
			clickAssembly = new Merge("click", new Pipe[]{clickAssemblyRTB, clickAssemblySVR});
			if(clickAssembly != null)
				clickAssembly = new Unique(clickAssembly, new Fields("click_jid"), 100000);
				
			Pipe picAssembly = null;
			Pipe picCount = null;
			if (!(clickAssembly==null) && !(piAssembly==null)){
			    // Defining the kept fields after the data is merged.

				Fields keptFields = new Fields("date0","timestamp1","campaign_name","jid1","campaign_id","adgroup_id1","creative_id1",
					"cth","ctw","domain_name","did","ip_address","user_agent","cookie","gogle_id","platform_type1",  
					"st_camp_id", "st_adg_id", "st_crtv_id","click_jid","click_date","click_timestamp",
					"click_flag1","imp_flag1","winning_price","errflg","flg","gfid","devlat","devlon","log","wp_bucket");
							
			    // Building the pipe to join the data
			    Fields impKeyFields = new Fields("jid1");
			    Fields clickKeyFields = new Fields("click_jid");
//			    Fields impKeyFields = new Fields("jid","service_type");
//			    Fields clickKeyFields = new Fields("click_jid","click_service_type");
			    //Joining impAssembly and clickAssembly
			    picAssembly = new CoGroup("icp_data", piAssembly[0], impKeyFields, clickAssembly, clickKeyFields, new LeftJoin());
			    Function<?> clickFunc = new AssignClickImpInt(new Fields("click_flag1", "imp_flag1"));
			    picAssembly = new Each(picAssembly, new Fields("click_jid", "jid1"), clickFunc, Fields.ALL);
				Filter<?> filterOutNull = new FilterOutNullData();
				picAssembly = new Each(picAssembly, new Fields("jid1"), filterOutNull); 
			    //Keeping the fields defined in keptFields.
			    picAssembly = new Each(picAssembly, keptFields, new Identity());

				SumBy sum1 = new SumBy(new Fields("imp_flag1"), new Fields("sum_imp"), int.class); 
				SumBy sum2 = new SumBy(new Fields("click_flag1"), new Fields("sum_click"), int.class); 
				picCount = new AggregateBy("imp_click_count", new Pipe[]{picAssembly}, new Fields("adgroup_id1"), sum1, sum2); 
			}
			setTails(picAssembly, piAssembly[1], piAssembly[2], picCount, clickAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}	
}
