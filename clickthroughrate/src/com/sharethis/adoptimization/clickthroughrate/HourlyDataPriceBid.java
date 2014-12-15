package com.sharethis.adoptimization.clickthroughrate;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Unique;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.CTRConstants;


/**
 * This is the assembly to read price confirmation data.
 */

public class HourlyDataPriceBid extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataPriceBid.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public HourlyDataPriceBid(Map<String, Tap> sources, String pbFilePath, String pbFileName, 
			String pDateStr, int hour, int numOfHoursImp, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{		
			// Defining the fields for the price and bid file.
			Fields pbFields = new Fields("pb_timestamp","pb_adgroup_id","pb_creative_id","winning_price","wp_bucket",
					"pb_jid","adslot_id","adslotvisibility","pb_campaign_id","bid_price","city","pb_google_id",
					"pb_ip_address","pb_domain_name","user_browser","user_os","user_device","pb_deal_id",
					"setting_id","user_seg_list","user_seg_id","vertical_list","min_bid_price","platform_type",
					"geo_target_id","mobile","model_id","model_score","gid_age","audience_id","crtv_size","sqi_bk",
					"appId","carrierId","deviceType","isApp","isInterstitialReqauest","platform","screenOrientation",
					"deviceId","location","device_make","device_model","deviceIdType");
			
            Class[] types = new Class[]{String.class, String.class, String.class, double.class, int.class,
            		String.class, String.class, String.class, String.class, double.class, String.class, String.class, 
            		String.class, String.class, String.class, String.class, String.class, String.class,
            		String.class, String.class, String.class, String.class, long.class, String.class,
            		String.class, String.class, String.class, double.class, long.class, String.class,int.class,int.class,
            		String.class, String.class, String.class, String.class, String.class, String.class, String.class,
            		String.class, String.class, String.class, String.class, String.class};    

    		Date pDate = sdf.parse(pDateStr);
            Scheme pbSourceScheme = new TextDelimited(pbFields, false, mDelimiter, types);
            String pipeName = "price_bid_" + CTRConstants.hourFolders[hour];	
            Pipe pbAssembly = new Pipe(pipeName);
            List<Tap> pbTapL = new ArrayList<Tap>();
            int ind = 0;
            for(int i=0; i<numOfHoursImp; i++){
            	int i_temp = i+hour-1;
        		Date pDate_temp = DateUtils.addHours(pDate, i_temp);
        		String pDateStr_temp = sdf.format(pDate_temp);
        		String dayStr_temp = pDateStr_temp.substring(0,4)+pDateStr_temp.substring(5,7)+pDateStr_temp.substring(8,10);
				if (i_temp > 23) {					
					i_temp = i_temp-24;
				}else{
					if(i_temp < 0)
						i_temp = 24 + i_temp;
				}
//				String filePathHourly = pbFilePath + dayStr_temp + "/" + CTRConstants.hourFolders[i_temp] + "/" + pbFileName + "/";
				String filePathHourly = pbFilePath + dayStr_temp + CTRConstants.hourFolders[i_temp] + "/" + pbFileName + "/";

           	    // Building the taps from the data files
            	Tap cTap = new Hfs(pbSourceScheme, filePathHourly);
    			JobConf	jobConf = new JobConf();
    			if(cTap.resourceExists(jobConf)){
    				sLogger.info(filePathHourly + " is used.");
    				pbTapL.add(ind, cTap);
    				ind++;
    			}else{
    				sLogger.info("The data path: " + filePathHourly + " does not exist.");				    				
    			}    				
            }	
            int tapLen = pbTapL.size();
            if(tapLen>0){
            	Tap[] pbTap = new Tap[tapLen];
            	for(int i=0; i<tapLen; i++)
            		pbTap[i] = pbTapL.get(i);
            	// Defining source Taps
				MultiSourceTap cSources = new MultiSourceTap(pbTap);
				sources.put(pipeName, cSources);
				pbAssembly = new Unique(pbAssembly, new Fields("pb_jid"), 100000);
				setTails(pbAssembly);            
            }else{
            	sLogger.info("ProcessingDate: " + pDateStr + " and the hour: " + hour);
            	setTails(new Pipe("No_Data", null));
            }
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
