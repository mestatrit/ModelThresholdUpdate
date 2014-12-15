package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import cascading.operation.Function;
import cascading.operation.Identity;
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

public class HourlyBucketDataForModelValidation extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyBucketDataForModelValidation.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
	
	public HourlyBucketDataForModelValidation(Map<String, Tap> sources, String ctrMVFilePath, String baseFileName, 
			String pDateStr, int hour, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{		
			Fields dataFields = new Fields("jid", "adgroup_id", "creative_id", "imp_flag", "click_flag", "cost", 
					"ctr_pred", "e_cpm", "e_cpc", "ctr_pred_1");
			
            Class[] types = new Class[]{String.class, String.class, String.class, double.class, 
            		double.class, double.class, double.class, double.class, double.class, double.class};    

            Scheme icSourceScheme = new TextDelimited(dataFields, false, mDelimiter, types);

            String pipeName = config.get("MVFileName");	
            Pipe baseAssembly = new Pipe(pipeName);
        	String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			String filePathHourly = ctrMVFilePath + dayStr + CTRConstants.hourFolders[hour] + "/" + baseFileName + "/";
          	  
			// Building the taps from the data files
            Tap cTap = new Hfs(icSourceScheme, filePathHourly);
    		JobConf	jobConf = new JobConf();
    		if(cTap.resourceExists(jobConf)){
    			sLogger.info(filePathHourly + " is used.");
 				sources.put(pipeName, cTap);

 				Fields keptFields = new Fields("jid", "adgroup_id", "creative_id", "imp_flag", 
 						"click_flag", "cost", "ctr_pred");
				baseAssembly = new Each(baseAssembly, keptFields, new Identity());				

				String ctrMVNameStr = config.get("CTRPMVNames");
				String ctrMVFieldStr = config.get("CTRPMVFields");
				String[] ctrMVNames = StringUtils.split(ctrMVNameStr, ";");
				String[] ctrMVFields = StringUtils.split(ctrMVFieldStr, ";");
				String mvBucketFieldName = config.get("CTRPBucketId");
				Fields mvBucketField = new Fields(mvBucketFieldName);
				String ctrThValList = config.get("CTRPThVals");
				String[] ctrThValStr = StringUtils.split(ctrThValList, ",");
				double[] ctrThVals = new double[ctrThValStr.length];
				for(int i=0; i<ctrThValStr.length; i++)
					ctrThVals[i] = Double.parseDouble(ctrThValStr[i]);
				int ctrInd = keptFields.getPos("ctr_pred");
				Function<?> oneFunc = new BucketizingPredCTR(mvBucketField, ctrInd, ctrThVals);
				baseAssembly = new Each(baseAssembly, keptFields, oneFunc, Fields.ALL);
				Pipe[] ctrMVAssembly = new Pipe[ctrMVNames.length];
				for(int i=0; i<ctrMVNames.length; i++){
					String[] keyFieldsArr = StringUtils.split(ctrMVFields[i], ",");
					Fields keyFields = new Fields();
					for(int j=0; j<keyFieldsArr.length;j++)
						keyFields = keyFields.append(new Fields(keyFieldsArr[j]));
					ctrMVAssembly[i] = new Pipe(ctrMVNames[i], baseAssembly);
					ctrMVAssembly[i] = new HourlyAggregatingPredCTR(
							ctrMVAssembly[i], ctrMVNames[i], keyFields).getTails()[0];
				}
				setTails(ctrMVAssembly);            
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
