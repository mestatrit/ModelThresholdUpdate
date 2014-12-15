package com.sharethis.adoptimization.campaigndata;

import cascading.operation.Function;
import cascading.pipe.Each;
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
import com.sharethis.adoptimization.common.AssignUserIdNew;


/**
 * This is the assembly to read success bid data.
 */

public class HourlyDataSuccessBid extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataSuccessBid.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public HourlyDataSuccessBid(Map<String, Tap> sources, String sbFilePath, String sbFileName, 
			String pDateStr, int hour, int numOfHoursBid, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{		
			// Defining the fields for the success bid file.
	        Fields sbFields = new Fields("sb_timestamp","adslot_id","adslotvisibility","sb_campaign_id","sb_adgroup_id",
	            	"sb_creative_id","bid_price","result_reason","city","sb_google_id","sb_ip_address",
					"sb_domain_name","user_browser","user_os","user_device","sb_jid","deal_id",
					"setting_id","user_list","vertical_list","min_bid_price","platform_type",
					"geo_target_id","mobile","model_id","model_score","gid_age","audience_id","crtv_size","sqi_bk", 
					"appId","carrierId","deviceType","isApp","isInterstitialReqauest","platform","screenOrientation",
					"deviceId","location","device_make","device_model","deviceIdType","seller_id");
			
            Class[] types = new Class[]{String.class, String.class, String.class, String.class, String.class,
            		String.class, double.class, String.class, String.class, String.class, String.class, 
            		String.class, String.class, String.class, String.class, String.class, String.class,
            		String.class, String.class, String.class, long.class, String.class,
            		String.class, String.class, String.class, double.class, long.class, String.class,int.class,int.class,
            		String.class, String.class, String.class, String.class, String.class, String.class, String.class,
            		String.class, String.class, String.class, String.class, String.class, String.class};    

    		Date pDate = sdf.parse(pDateStr);
            Scheme sbSourceScheme = new TextDelimited(sbFields, false, mDelimiter, types);
            String pipeName = "success_bid_" + CTRConstants.hourFolders[hour];	
            Pipe sbAssembly = new Pipe(pipeName);
            List<Tap> sbTapL = new ArrayList<Tap>();
            int ind = 0;
            for(int i=0; i<numOfHoursBid; i++){
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
				String filePathHourly = sbFilePath + dayStr_temp + CTRConstants.hourFolders[i_temp] + "/" + sbFileName + "/";

           	    // Building the taps from the data files
            	Tap cTap = new Hfs(sbSourceScheme, filePathHourly);
    			JobConf	jobConf = new JobConf();
    			if(cTap.resourceExists(jobConf)){
    				sLogger.info(filePathHourly + " is used.");
    				sbTapL.add(ind, cTap);
    				ind++;
    			}else{
    				sLogger.info("The data path: " + filePathHourly + " does not exist.");				    				
    			}    				
            }	
            int tapLen = sbTapL.size();
            if(tapLen>0){
            	Tap[] pbTap = new Tap[tapLen];
            	for(int i=0; i<tapLen; i++)
            		pbTap[i] = sbTapL.get(i);
            	// Defining source Taps
				MultiSourceTap cSources = new MultiSourceTap(pbTap);
				sources.put(pipeName, cSources);
				sbAssembly = new Unique(sbAssembly, new Fields("sb_jid"), 100000);
				
//				Function<?> assignUser = new AssignUserIdNew(new Fields("user_seg_list","user_seg_id"));
//				sbAssembly = new Each(sbAssembly, new Fields("user_list"), assignUser, Fields.ALL);	

				setTails(sbAssembly);            
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
