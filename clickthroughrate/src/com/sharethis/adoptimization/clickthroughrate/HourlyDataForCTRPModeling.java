package com.sharethis.adoptimization.clickthroughrate;

import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.filter.Not;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.joiner.LeftJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ExpandingDataKeyCategory;
import com.sharethis.adoptimization.common.ExpandingDataVerticalList;
import com.sharethis.adoptimization.common.ExpandingDataUserList;
import com.sharethis.adoptimization.common.FilterOutDataNEInt;
import com.sharethis.adoptimization.common.FilterOutNullData;


/**
 * This is the assembly to join the bidder and price confirmation data.
 */

public class HourlyDataForCTRPModeling extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataForCTRPModeling.class);
	private static final long serialVersionUID = 1L;

	public HourlyDataForCTRPModeling(Pipe modAssembly, String impFilePath, String clickFilePath, 
			String pDateStr, int hour, int numOfHoursClick, String pbFilePath, String pbFileName, 
			int numOfHoursImp, String mapPipeName, String mapKeys, Configuration config) 
					throws IOException, ParseException, Exception
	{
		try{				
		    Pipe modSampAssembly = null;
		    int numOfMaps = 0;
		    if (!(modAssembly==null)){
	            // Defining the kept fields.
		    	Fields keptFields = new Fields("winning_price", "wp_bucket", "adslot_id", "adslotvisibility", "city", 
		    			"pb_google_id", "setting_id", "user_seg_list", "vertical_list", "date", "timestamp", 
		    			"campaign_name", "jid", "campaign_id", "adgroup_id", "creative_id", "domain_name", 
		    			"deal_id", "ip_address", "user_agent", "cookie", "service_type", "st_camp_id", 
		    			"st_adg_id", "st_crtv_id", "click_flag1", "imp_flag1", "click_timestamp");
							
				modSampAssembly = new Pipe("sampeled_data", modAssembly);

				SumBy sum1 = new SumBy(new Fields("imp_flag1"), new Fields("imp_cnt"), double.class); 
				Pipe cntAssembly = new AggregateBy("cnt_data", new Pipe[]{modSampAssembly}, new Fields("click_flag1"), sum1); 
				
				//TODO: Separating the data with/without a click
				Filter<?> filterOut = new FilterOutDataNEInt(1);
				Pipe modSampAssembly_c = new Each(modSampAssembly, new Fields("click_flag1"), filterOut); 				
				Pipe modSampAssembly_nc = new Each(modSampAssembly, new Fields("click_flag1"), new Not(filterOut)); 
				
				//TODO: Counting the number clicks
				
				//TODO: Sampling the data based on the ratio of non-clicks vs clicks. The ratio is defined in the property file
				//TODO: Randomly choosing one vertical and/or user segment 
				//      and/or ShareThis vertical category and/or ShareThis creative category
				
				
				/*****************************/
				//Expanding the data for vertical list.
				int vFieldInd_tmp = keptFields.getPos("vertical_list");

				/*****************************/
				//Expanding the data for ShareThis vertical category.
				ClickThroughRateUtils ctrUtils = new ClickThroughRateUtils(config);
				Map<String, List<String>> setCatMap = ctrUtils.AdSpaceCategory();
				int setFieldInd = keptFields.getPos("setting_id");
				/*****************************/
				//Expanding the data for user list.
				int uFieldInd = keptFields.getPos("user_seg_list");
				
				/*****************************/
				//Expanding the data for ShareThis creative category.
				Map<String, List<String>> crtvCatMap = ctrUtils.CreativeCategory();
				int crtvFieldInd = keptFields.getPos("creative_id");
				/*****************************/												
		    }
		    Pipe[] resAssembly = new Pipe[numOfMaps+2];
		    resAssembly[numOfMaps+1] = modSampAssembly;
		    setTails(resAssembly);		    	
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}		
}
