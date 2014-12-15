package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import cascading.operation.Function;
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
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.CTRConstants;


/**
 * This is the assembly to read price confirmation data.
 */

public class HourlyDataForModelValidation extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataForModelValidation.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
	
	public HourlyDataForModelValidation(Map<String, Tap> sources, String pbFilePath, String icFileName, 
			String pDateStr, int hour, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{		
			Fields dataFields = new Fields("winning_price", "wp_bucket", "adslot_id", "adslotvisibility", "city", 
					"pb_google_id", "setting_id", "user_seg_id", "date", "timestamp", "campaign_name", "jid", "campaign_id", 
					"adgroup_id", "creative_id", "domain_name", "deal_id", "ip_address", "user_agent", "cookie", 
					"service_type", "st_camp_id", "st_adg_id", "st_crtv_id", 
					"min_bid_price", "platform_type", "geo_target_id", "mobile", "model_id", "model_score",
					"user_browser","user_os","user_device","crtv_size","sqi","errflg","flg",
					"appId","carrierId","deviceType","isApp","isInterstitialReqauest","platform","screenOrientation",
					"deviceId","location","device_make","device_model","deviceIdType",
					"vertical_id", "vertical_wt", "asl1_id", "ctl1_id", "click_flag", "imp_flag", "cost", "usg_age_hour", "gid_age_hour");			
			
            Class[] types = new Class[]{double.class, String.class, String.class, String.class, String.class, 
            		String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, 
            		String.class, String.class, String.class, String.class, String.class, String.class, String.class, 
            		String.class, String.class, String.class, String.class, 
            		long.class, String.class, String.class, String.class, String.class, double.class,
            		String.class, String.class, String.class, String.class, String.class, int.class, int.class,
            		String.class, String.class, String.class, String.class, String.class, String.class,
            		String.class, String.class, String.class, String.class, String.class, String.class, 
            		String.class, double.class, String.class, String.class, double.class, 
            		double.class, double.class, long.class, long.class};    

            Scheme icSourceScheme = new TextDelimited(dataFields, false, mDelimiter, types);

            String pipeName = config.get("CTRPFileName");	
            Pipe icAssembly = new Pipe(pipeName);
        	String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			String filePathHourly = pbFilePath + dayStr + CTRConstants.hourFolders[hour] + "/" + icFileName + "/";
          	  
			// Building the taps from the data files
            Tap cTap = new Hfs(icSourceScheme, filePathHourly);
    		JobConf	jobConf = new JobConf();
    		if(cTap.resourceExists(jobConf)){
    			sLogger.info(filePathHourly + " is used.");
 				sources.put(pipeName, cTap);

				CTRPQueryingModel qModel = new CTRPQueryingModel();
				Map<String, Double> modelMap = qModel.quaryingModel(config);
				double[] coeffs = qModel.getModelCoeffs();
				double intercept = qModel.getIntercept();				
			    String[] keyNames = qModel.getKeyNames();
			    String[] keyIds = qModel.getKeyIds();
				CTRPQueryingCTRBase qCTR = new CTRPQueryingCTRBase();
				Map<String, Map<String,Double>> ctrBaseMap = qCTR.quaryingCTRBase(config, keyNames);
				
				String impField = "imp_flag";
				Fields resultFields = new Fields("ctr_pred");

				//sLogger.info("CTR base map: " + ctrBaseMap.toString());

				Function<?> predFunc = new PredictingCTRforModelValidation(resultFields, keyNames, 
						keyIds, coeffs, intercept, ctrBaseMap, impField);
				icAssembly = new Each(icAssembly, dataFields, predFunc, Fields.ALL);		    		
						
				String ctrPredPipeNameStr = config.get("CTRPAggNames");
				String ctrAggFieldStr = config.get("CTRPAggFields");
				String[] ctrPredPipeNames = StringUtils.split(ctrPredPipeNameStr, ";");
				String[] ctrAggFields = StringUtils.split(ctrAggFieldStr, ";");
				Pipe[] ctrPredAssembly = new Pipe[ctrPredPipeNames.length+1];
				for(int i=0; i<ctrPredPipeNames.length; i++){
					String[] keyFieldsArr = StringUtils.split(ctrAggFields[i], ",");
					Fields keyFields = new Fields();
					for(int j=0; j<keyFieldsArr.length;j++)
						keyFields = keyFields.append(new Fields(keyFieldsArr[j]));
					ctrPredAssembly[i] = new Pipe(ctrPredPipeNames[i], icAssembly);
					ctrPredAssembly[i] = new HourlyAggregatingPredCTR(ctrPredAssembly[i], ctrPredPipeNames[i], keyFields).getTails()[0];
				}
								
				ctrPredAssembly[ctrPredPipeNames.length] = icAssembly;
				setTails(ctrPredAssembly);            
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
