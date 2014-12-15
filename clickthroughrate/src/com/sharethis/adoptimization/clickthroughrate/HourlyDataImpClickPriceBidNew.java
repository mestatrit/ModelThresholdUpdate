package com.sharethis.adoptimization.clickthroughrate;

import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.filter.Not;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConvertingSecondToHour;
import com.sharethis.adoptimization.common.ExpandingDataKeyCategory;
import com.sharethis.adoptimization.common.ExpandingDataNewUserList;
import com.sharethis.adoptimization.common.ExpandingDataVerticalList;
import com.sharethis.adoptimization.common.FilterOutDataNEInt;


/**
 * This is the assembly to join the bidder and price confirmation data.
 */

public class HourlyDataImpClickPriceBidNew extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataImpClickPriceBidNew.class);
	private static final long serialVersionUID = 1L;

	public HourlyDataImpClickPriceBidNew(Map<String, Tap> sources, String allFilePath, String allFileName,
			String pDateStr, int hour, String mapPipeName, String mapKeys, Configuration config) 
			throws IOException, ParseException, Exception
	{
		try{				
		    Pipe allAssembly = new HourlyDataAll(sources, allFilePath, allFileName, pDateStr, 
					hour, config).getTails()[0];

		    Pipe[] mapAssembly = null;
	//	    Pipe cntAssembly = null;
	//	    Pipe modSampAssembly_c = null;
	//	    Pipe modSampAssembly_nc = null;
		    		
		    int numOfMaps = 0;
		    if (!(allAssembly==null)){
	            // Defining the kept fields.
		    	Fields keptFields = new Fields("winning_price", "wp_bucket", "adslot_id", "adslotvisibility", "city", 
		    			"pb_google_id", "setting_id", "user_seg_list", "vertical_list", "date", "timestamp", 
		    			"campaign_name", "jid", "campaign_id", "adgroup_id", "creative_id", "domain_name", 
		    			"deal_id", "ip_address", "user_agent", "cookie", "service_type", "st_camp_id", 
		    			"st_adg_id", "st_crtv_id", "click_flag1", "imp_flag1", "click_timestamp", 
		    			"min_bid_price","platform_type","geo_target_id","mobile","model_id","model_score",
		    			"gid_age","audience_id","user_browser","user_os","user_device","crtv_size","sqi_bk","errflg","flg",
						"appId","carrierId","deviceType","isApp","isInterstitialReqauest","platform","screenOrientation",
						"deviceId","location","device_make","device_model","deviceIdType");
							
//				Pipe modSampAssembly = new Pipe("data_all", allAssembly);
				
				/*****************************/
				//Expanding the data for vertical list.
				int vFieldInd_tmp = keptFields.getPos("vertical_list");
				int cFieldInd_tmp = keptFields.getPos("click_flag1");
				int iFieldInd_tmp = keptFields.getPos("imp_flag1");
				int pFieldInd_tmp = keptFields.getPos("winning_price");
				sLogger.info("The position of vertical_list: " + vFieldInd_tmp + "   click_flag1: " + cFieldInd_tmp 
						+ "   imp_flag1: " + iFieldInd_tmp + "   winning_price: " + pFieldInd_tmp);
				Function<?> exFunc = new ExpandingDataVerticalList(keptFields.append(new Fields("vertical_id", "vertical_wt", 
						"click_flag12", "imp_flag12", "cost12")), keptFields, vFieldInd_tmp, cFieldInd_tmp, iFieldInd_tmp, pFieldInd_tmp);			
				allAssembly = new Each(allAssembly, keptFields, exFunc, Fields.RESULTS);	

				Fields keptFields0 = new Fields("winning_price", "wp_bucket", "adslot_id", "adslotvisibility", "city", 
						"pb_google_id", "setting_id", "user_seg_list", "date", "timestamp", "campaign_name", "jid", "campaign_id", 
						"adgroup_id", "creative_id", "domain_name", "deal_id", "ip_address", "user_agent", "cookie", 
						"service_type", "st_camp_id", "st_adg_id", "st_crtv_id", 
						"min_bid_price", "platform_type","geo_target_id","mobile","model_id", "model_score", "gid_age", "audience_id",
						"user_browser","user_os","user_device","crtv_size","sqi_bk","errflg","flg",
						"appId","carrierId","deviceType","isApp","isInterstitialReqauest","platform","screenOrientation",
						"deviceId","location","device_make","device_model","deviceIdType",
						"vertical_id", "vertical_wt", "click_flag12", "imp_flag12", "cost12");
				allAssembly = new Each(allAssembly, keptFields0, new Identity());				
				/*****************************/		

				String[] mapNamesArr = StringUtils.split(mapPipeName, ";");
				String[] mapKeyStrArr = StringUtils.split(mapKeys, ";");
				if(mapNamesArr!=null){
					numOfMaps = mapNamesArr.length;
					mapAssembly = new Pipe[numOfMaps];
					for(int i_m=0; i_m<numOfMaps; i_m++){
						String[] mapKeyStr = StringUtils.split(mapKeyStrArr[i_m], ",");
						Fields mapKeyFields = new Fields();
						for(int i=0; i<mapKeyStr.length; i++)
							mapKeyFields = mapKeyFields.append(new Fields(mapKeyStr[i]));
						mapAssembly[i_m] = new Pipe(mapNamesArr[i_m], allAssembly);				
						mapAssembly[i_m] = new MappingsHourlySubassembly(mapAssembly[i_m], mapNamesArr[i_m], mapKeyFields, 
								"vertical_wt", "imp_flag12").getTails()[0];
					}
				}

				/*****************************/
				//Expanding the data for ShareThis vertical category.
				ClickThroughRateUtils ctrUtils = new ClickThroughRateUtils(config);
				Map<String, List<String>> setCatMap = ctrUtils.AdSpaceCategory();
				int setFieldInd = keptFields0.getPos("setting_id");
				int cFieldInd = keptFields0.getPos("click_flag12");
				int iFieldInd = keptFields0.getPos("imp_flag12");
				int pFieldInd = keptFields0.getPos("cost12");
				sLogger.info("The position of click_flag12: " + cFieldInd 
						+ "   imp_flag12: " + iFieldInd + "   cost12: " + pFieldInd);
				Function<?> setFunc = new ExpandingDataKeyCategory(keptFields0.append(new Fields("asl1_id", 
						"click_flag2", "imp_flag2", "cost2")), keptFields0, setFieldInd, cFieldInd, iFieldInd, pFieldInd, setCatMap);			
				allAssembly = new Each(allAssembly, keptFields0, setFunc, Fields.RESULTS);	

				Fields keptFields1 = new Fields("winning_price", "wp_bucket", "adslot_id", "adslotvisibility", "city", 
						"pb_google_id", "setting_id", "user_seg_list", "date", "timestamp", "campaign_name", "jid", "campaign_id", 
						"adgroup_id", "creative_id", "domain_name", "deal_id", "ip_address", "user_agent", "cookie", 
						"service_type", "st_camp_id", "st_adg_id", "st_crtv_id", 
						"min_bid_price", "platform_type","geo_target_id","mobile","model_id", "model_score", "gid_age", "audience_id",
						"user_browser","user_os","user_device","crtv_size","sqi_bk","errflg","flg",
						"appId","carrierId","deviceType","isApp","isInterstitialReqauest","platform","screenOrientation",
						"deviceId","location","device_make","device_model","deviceIdType",
						"vertical_id", "vertical_wt", "asl1_id", "click_flag2", "imp_flag2", "cost2");
				allAssembly = new Each(allAssembly, keptFields1, new Identity());				
				/*****************************/		
				
				/*****************************/
				//Expanding the data for user list.
				int uFieldInd = keptFields1.getPos("user_seg_list");
				cFieldInd = keptFields1.getPos("click_flag2");
				iFieldInd = keptFields1.getPos("imp_flag2");
				pFieldInd = keptFields1.getPos("cost2");
				sLogger.info("The position of user_seg_list: " + uFieldInd);
				Function<?> userFunc = new ExpandingDataNewUserList(keptFields1.append(new Fields("user_seg_id", "usg_age", 
						"click_flag3", "imp_flag3", "cost3")), keptFields1, uFieldInd, cFieldInd, iFieldInd, pFieldInd);			
				allAssembly = new Each(allAssembly, keptFields1, userFunc, Fields.RESULTS);	
				
				Fields keptFields2 = new Fields("winning_price", "wp_bucket", "adslot_id", "adslotvisibility", "city", 
						"pb_google_id", "setting_id", "user_seg_id", "date", "timestamp", "campaign_name", "jid", "campaign_id", 
						"adgroup_id", "creative_id", "domain_name", "deal_id", "ip_address", "user_agent", "cookie", 
						"service_type", "st_camp_id", "st_adg_id", "st_crtv_id", 
						"min_bid_price", "platform_type","geo_target_id","mobile","model_id", "model_score", "gid_age", "audience_id",
						"user_browser","user_os","user_device","crtv_size","sqi_bk","errflg","flg",
						"appId","carrierId","deviceType","isApp","isInterstitialReqauest","platform","screenOrientation",
						"deviceId","location","device_make","device_model","deviceIdType",
						"vertical_id", "vertical_wt", "asl1_id", "click_flag3", "imp_flag3", "cost3", "usg_age");
				allAssembly = new Each(allAssembly, keptFields2, new Identity());				
				/*****************************/				
				
				/*****************************/
				//Expanding the data for ShareThis creative category.
				Map<String, List<String>> crtvCatMap = ctrUtils.CreativeCategory();
				int crtvFieldInd = keptFields2.getPos("creative_id");
				cFieldInd = keptFields2.getPos("click_flag3");
				iFieldInd = keptFields2.getPos("imp_flag3");
				pFieldInd = keptFields2.getPos("cost3");
				sLogger.info("The position of creative_id: " + crtvFieldInd);
				Function<?> crtvFunc = new ExpandingDataKeyCategory(keptFields2.append(new Fields("ctl1_id", 
						"click_flag", "imp_flag", "cost")), keptFields2, crtvFieldInd, cFieldInd, iFieldInd, pFieldInd, crtvCatMap);			
				allAssembly = new Each(allAssembly, keptFields2, crtvFunc, Fields.RESULTS);	
				
				Function<?> convFunc = new ConvertingSecondToHour(new Fields("usg_age_hour","gid_age_hour"));
				allAssembly = new Each(allAssembly, new Fields("usg_age","gid_age"), convFunc, Fields.ALL);	
				
				Fields keptFields3 = new Fields("winning_price", "wp_bucket", "adslot_id", "adslotvisibility", "city", 
						"pb_google_id", "setting_id", "user_seg_id", "date", "timestamp", "campaign_name", "jid", "campaign_id", 
						"adgroup_id", "creative_id", "domain_name", "deal_id", "ip_address", "user_agent", "cookie", 
						"service_type", "st_camp_id", "st_adg_id", "st_crtv_id", 
						"min_bid_price", "platform_type","geo_target_id","mobile","model_id", "model_score",
						"user_browser","user_os","user_device","crtv_size","sqi_bk","errflg","flg",
						"appId","carrierId","deviceType","isApp","isInterstitialReqauest","platform","screenOrientation",
						"deviceId","location","device_make","device_model","deviceIdType",
						"vertical_id", "vertical_wt", "asl1_id", "ctl1_id", "click_flag", "imp_flag", "cost", "usg_age_hour", "gid_age_hour");
				allAssembly = new Each(allAssembly, keptFields3, new Identity());				
				/*****************************/			
/*				
				String modDataClick = config.get("ModDataClick");
				String modDataNonClick = config.get("ModDataNonClick");
				String modDataCount = config.get("ModDataCount");

				Filter<?> filterOut = new FilterOutDataNEInt(1);
				modSampAssembly_c = new Pipe(modDataClick, new Each(modSampAssembly, new Fields("click_flag1"), filterOut)); 				
				modSampAssembly_nc = new Pipe(modDataNonClick, new Each(modSampAssembly, new Fields("click_flag1"), new Not(filterOut))); 
				SumBy sum1 = new SumBy(new Fields("imp_flag1"), new Fields("imp_cnt"), double.class); 
				cntAssembly = new AggregateBy(modDataCount, new Pipe[]{modSampAssembly}, new Fields("click_flag1"), sum1); 
*/
		    }
		    Pipe[] resAssembly = new Pipe[numOfMaps+1];
		    resAssembly[0] = new Pipe(config.get("IcpFileName"), allAssembly);
		    for(int i=0; i<numOfMaps; i++)
		    	resAssembly[i+1] = mapAssembly[i];
//		    resAssembly[numOfMaps+1] = cntAssembly;
//		    resAssembly[numOfMaps+2] = modSampAssembly_c;
//		    resAssembly[numOfMaps+3] = modSampAssembly_nc;
		    
		    setTails(resAssembly);		    	
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}		
}
