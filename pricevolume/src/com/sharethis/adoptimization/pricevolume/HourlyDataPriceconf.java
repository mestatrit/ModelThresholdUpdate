package com.sharethis.adoptimization.pricevolume;

import cascading.operation.Filter;
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

import com.sharethis.adoptimization.common.FilterOutNullData;
import com.sharethis.adoptimization.common.PVConstants;


/**
 * This is the assembly to read price confirmation data.
 */

public class HourlyDataPriceconf extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataPriceconf.class);
	private static final long serialVersionUID = 1L;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static String mDelimiter = "\t";
	
	public HourlyDataPriceconf(Map<String, Tap> sources, String priceFilePath, String pDateStr, int hour, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{		
			// Defining the fields of price data
            Fields priceFields = new Fields("date0","type","adgroup_id1","creative_id1","winning_price1",
            		"jid1","timestamp1","platform_type1","reference_id","pc_type", "pc_message");	
            Class[] types = new Class[]{String.class, String.class, String.class, String.class, String.class, 
            		String.class, String.class, String.class, String.class, String.class, String.class};           
            String hourFolder = PVConstants.hourFolders[hour]; 
            String pipeName = "priceSource_" + hourFolder;

    		Date pDate = sdf.parse(pDateStr);
            // Building the taps from the data files
//			Scheme priceSourceScheme = new TextDelimited(priceFields, false, mDelimiter, types);
			Scheme priceSourceScheme = new TextDelimited(priceFields, null, false, false, mDelimiter, false, "", types, true);
            List<Tap> priceTapL = new ArrayList<Tap>();
            int ind = 0;
            for(int i=0; i<3; i++){
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
				//String filePathHourly = priceFilePath + dayStr_temp + "/" + PVConstants.hourFolders[i_temp] + "/";

				// The change is to handle the new data path: yyyyMMddhh
				String filePathHourly = priceFilePath + dayStr_temp + PVConstants.hourFolders[i_temp] + "/";

				// Building the taps from the data files
            	Tap cTap = new Hfs(priceSourceScheme, filePathHourly);
    			JobConf	jobConf = new JobConf(config);
    			if(cTap.resourceExists(jobConf)){
    				sLogger.info(filePathHourly + " is used.");
    				priceTapL.add(ind, cTap);
    				ind++;
    			}else{
    				sLogger.info("The data path: " + filePathHourly + " does not exist.");				    				
    			}    				
            }	
            int tapLen = priceTapL.size();
            Pipe priceAssembly = null;
            if(tapLen>0){
            	priceAssembly = new Pipe(pipeName);
            	Tap[] priceTap = new Tap[tapLen];
            	for(int i=0; i<tapLen; i++)
            		priceTap[i] = priceTapL.get(i);
            	// Defining source Taps
				MultiSourceTap cSources = new MultiSourceTap(priceTap);
				sources.put(pipeName, cSources);
				double bucketWidth = config.getFloat("BucketWidth", 0.1F);
				Function<?> adj_wp = new AdjustingWinningPrice(new Fields("winning_price", "wp_bucket"), 1000.0, bucketWidth);
				priceAssembly = new Each(priceAssembly, new Fields("winning_price1"), adj_wp, Fields.ALL);			
				Filter<?> filterOutNull = new FilterOutNullData();
				priceAssembly = new Each(priceAssembly, new Fields("jid1"), filterOutNull); 
				priceAssembly = new Unique(priceAssembly, new Fields("jid1"), 100000);
				setTails(priceAssembly);
			}else{
				throw new Exception("The priceconf data at hour " + hour + " on " + pDateStr + " does not exist.");
            }            

		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
