package com.sharethis.adoptimization.inventory;

import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.filter.Not;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.FilterOutNullData;
import com.sharethis.adoptimization.common.PVConstants;
import com.sharethis.adoptimization.pricevolume.ParsingDataFields;
import com.sharethis.adoptimization.pricevolume.PriceVolumeUtils;


/**
 * This is the assembly to read no bid data.
 */

public class HourlyInventory extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyInventory.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
		
	public HourlyInventory(Map<String, Tap> sources, String nobidFilePath, String pDateStr, int hour, Configuration config) 
			throws IOException, ParseException, Exception
	{
		try{				
			// Defining the fields of no bid data
			Fields bidFields = new Fields("date","ad","adslot_id","adslotvisibility",
					"campaign_id","crtv_height","crtv_width","adgroup_id","creative_id",
					"bid_price0","result_reason","city","country","google_id","ip_address",
					"model_id","reference_id","network","sharethis_id","url","user_agent",
					"jid","timestamp","deal_id","setting_id","user_list",
					"vertical_list","min_bid_price0","platform_type","geo_target_id","mobile");			
			
	        Class[] types = new Class[]{String.class, String.class, String.class, String.class, 
	            	String.class,int.class,int.class,String.class,String.class,
	            	double.class, String.class, String.class, String.class,String.class,String.class,
	            	int.class,int.class,String.class,String.class,String.class,String.class,
	            	String.class,String.class,String.class,String.class,String.class,
	            	String.class,long.class,String.class,String.class,String.class};           

	        Fields keptFields = new Fields("adslot_id","adslotvisibility","campaign_id","adgroup_id",
	            	"creative_id","bid_price0","result_reason","domain_name",
					"setting_id","user_list","min_bid_price0","platform_type");
	            	          
	        String hourFolder = PVConstants.hourFolders[hour]; 	        
	        String pipeName = "dataSource_" + hourFolder;    
	        Pipe dataAssembly = new Pipe(pipeName);
				
	        // Building the taps from the data files
	        String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);   
	        //String nobidFilePathHourly = nobidFilePath + dayStr + "/" + hourFolder;
	        String nobidFilePathHourly = nobidFilePath + dayStr + hourFolder;
			
			Scheme bidSourceScheme = new TextDelimited(bidFields, null, false, false, mDelimiter, false, "", types, true);
			
			Tap bidTap = new Hfs(bidSourceScheme, nobidFilePathHourly);

			JobConf	jobConf = new JobConf(config);
			if(bidTap.resourceExists(jobConf)){
				PriceVolumeUtils pvUtils = new PriceVolumeUtils(config);
				sLogger.info(nobidFilePathHourly + " exists.");
				sources.put(pipeName, bidTap);

				Filter<?> filter = new FilterOutNullData();
				dataAssembly = new Each(dataAssembly, new Fields("jid"), filter);

				Function<?> parser = new ParsingDataFields(new Fields("domain_name", "user_browser", "user_os", "user_device", "crtv_size"));
				dataAssembly = new Each(dataAssembly, new Fields("url","user_agent","crtv_height","crtv_width"), parser, Fields.ALL);			

				// Drop useless data fields/columns
				dataAssembly = new Each(dataAssembly, keptFields, new Identity());								
				
				//Expanding all data
				Map<String, List<String>> adgroupUserMap = pvUtils.adgroupUserMapping();
				Map<String, List<String>> catMap = pvUtils.CreativeCategory();
				Map<String, List<String>> setMap = pvUtils.AdSpaceCategory();
				
		        Fields keptFields0 = new Fields("adslot_id","adslotvisibility","campaign_id","adgroup_id","creative_id","result_reason",
		        		"domain_name","user_seg_id","ctl1_id","asl1_id","platform_type",
		        		"req_cnt","bid_price","min_bid_price", "max_cpm", "freq_cap");

		        Function<?> exData = new ExpandingInventoryAll(new Fields("req_cnt", "bid_price", "min_bid_price", 
						"ctl1_id", "asl1_id", "user_seg_id", "max_cpm", "freq_cap"), 1000.0, catMap, setMap, adgroupUserMap);
				dataAssembly = new Each(dataAssembly, new Fields("bid_price0", "min_bid_price0", 
						"creative_id", "setting_id", "adgroup_id", "user_list"), exData, keptFields0);			

/*				
				//*****************************
				//Expanding the data for user list.
				int aFieldInd = keptFields.getPos("adgroup_id");
				int uFieldInd = keptFields.getPos("user_list");
				int bpFieldInd = keptFields.getPos("bid_price0");
				int mbpFieldInd = keptFields.getPos("min_bid_price0");
				sLogger.info("The position of user_list: " + uFieldInd);
				Function<?> userFunc = new ExpandingInventoryUserList(keptFields.append(new Fields("user_seg_id", 
						"req_cnt1", "bid_price1", "min_bid_price1")), keptFields, 
						aFieldInd, uFieldInd, bpFieldInd, mbpFieldInd, adgroupUserMap, 1000.0);			
				dataAssembly = new Each(dataAssembly, keptFields, userFunc, Fields.RESULTS);						
		        Fields keptFields1 = new Fields("adslot_id","adslotvisibility","campaign_id","adgroup_id","creative_id",
		        		"result_reason","domain_name","setting_id","user_seg_id","req_cnt1","bid_price1","min_bid_price1");
				dataAssembly = new Each(dataAssembly, keptFields1, new Identity());								
				//*****************************				
										        
				//*****************************
				//Expanding the data for ShareThis creative category.
				int keyFieldInd = keptFields1.getPos("creative_id");
				int cFieldInd = keptFields1.getPos("req_cnt1");
				bpFieldInd = keptFields1.getPos("bid_price1");
				mbpFieldInd = keptFields1.getPos("min_bid_price1");
				sLogger.info("The position of creative_id: " + keyFieldInd);
				Function<?> catFunc = new ExpandingInventoryCategory(keptFields1.append(new Fields("ctl1_id", "req_cnt2", 
						"bid_price2", "min_bid_price2")), keptFields1, keyFieldInd, cFieldInd, bpFieldInd, mbpFieldInd, catMap);			
				dataAssembly = new Each(dataAssembly, keptFields1, catFunc, Fields.RESULTS);					
				Fields keptFields2 = new Fields("adslot_id","adslotvisibility","campaign_id", "adgroup_id","result_reason",
		        		"domain_name","setting_id","user_seg_id","ctl1_id","req_cnt2","bid_price2","min_bid_price2");
				dataAssembly = new Each(dataAssembly, keptFields2, new Identity());								
				//*****************************												

				//*****************************
				//Expanding the data for ShareThis creative category.
				keyFieldInd = keptFields2.getPos("setting_id");
				cFieldInd = keptFields2.getPos("req_cnt2");
				bpFieldInd = keptFields2.getPos("bid_price2");
				mbpFieldInd = keptFields2.getPos("min_bid_price2");
				sLogger.info("The position of setting_id: " + keyFieldInd);
				Function<?> setFunc = new ExpandingInventoryCategory(keptFields2.append(new Fields("asl1_id", "req_cnt", 
						"bid_price", "min_bid_price")), keptFields2, keyFieldInd, cFieldInd, bpFieldInd, mbpFieldInd, setMap);			
				dataAssembly = new Each(dataAssembly, keptFields2, setFunc, Fields.RESULTS);					
		        Fields keptFields3 = new Fields("adslot_id","adslotvisibility","campaign_id","adgroup_id","result_reason",
		        		"domain_name","user_seg_id","ctl1_id","asl1_id","req_cnt","bid_price","min_bid_price");
				dataAssembly = new Each(dataAssembly, keptFields3, new Identity());								
				//*****************************												
*/				
//				Tap bidTrap = new Hfs(bidSourceScheme, nobidFilePathHourly);
//				FlowDef flowDef = new FlowDef();
//			    flowDef.addSource(dataAssembly, bidTap);
//			    flowDef.addTrap(dataAssembly, bidTrap);

				setTails(dataAssembly);
			}else{
				throw new Exception("The data path: " + nobidFilePathHourly+ " does not exist.");				
			}
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}		
}
