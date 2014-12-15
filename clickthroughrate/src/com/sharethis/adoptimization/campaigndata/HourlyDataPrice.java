package com.sharethis.adoptimization.campaigndata;

import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Unique;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AdjustingWinningPrice;
import com.sharethis.adoptimization.common.CTRConstants;
import com.sharethis.adoptimization.common.FilterOutNullData;


/**
 * This is the assembly to read price confirmation data.
 */

public class HourlyDataPrice extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataPrice.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
	
	public HourlyDataPrice(Map<String, Tap> sources, String priceFilePath, String pDateStr, int hour, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{		
			// Defining the fields of price data
            Fields priceFields = new Fields("date0","type","adgroup_id1","creative_id1","winning_price1",
            		"jid1","timestamp1","platform_type1","reference_id","pc_type", "pc_message");	
            Class[] types = new Class[]{String.class, String.class, String.class, String.class, String.class, 
            		String.class, String.class, String.class, String.class, String.class, String.class};           
            
            String hourFolder = CTRConstants.hourFolders[hour]; 
            String pipeName = "price_" + hourFolder;
            Pipe priceAssembly = null;
            // Building the taps from the data files
			Scheme priceSourceScheme = new TextDelimited(priceFields, null, false, false, mDelimiter, false, "", types, true);
        	String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			// The change is to handle the new data path: yyyyMMddhh
			String filePathHourly = priceFilePath + dayStr + CTRConstants.hourFolders[hour] + "/";
			// Building the taps from the data files
            Tap cTap = new Hfs(priceSourceScheme, filePathHourly);
    		JobConf	jobConf = new JobConf(config);
    		if(cTap.resourceExists(jobConf)){
    			sLogger.info(filePathHourly + " is used.");
				priceAssembly = new Pipe(pipeName);
				sources.put(pipeName, cTap);
				Pipe rawPCCount = new Pipe("pc_count", new CountBy(priceAssembly, new Fields("adgroup_id1"),  new Fields("raw_pc_count")));

				double bucketWidth = config.getFloat("BucketWidth", 0.1F);
				Function<?> adj_wp = new AdjustingWinningPrice(new Fields("winning_price", "wp_bucket"), 1000.0, bucketWidth);
				priceAssembly = new Each(priceAssembly, new Fields("winning_price1"), adj_wp, Fields.ALL);			
				
				Filter<?> filterOutNull = new FilterOutNullData();
				priceAssembly = new Each(priceAssembly, new Fields("jid1"), filterOutNull); 
				priceAssembly = new Unique(priceAssembly, new Fields("jid1"), 100000);
				
				// Defining the kept fields
	            Fields keptFields = new Fields("date0","type","adgroup_id1","creative_id1","winning_price","wp_bucket",
	            		"jid1","timestamp1","platform_type1");	
	            priceAssembly = new Each(priceAssembly, keptFields, new Identity());				
				Pipe pcCount = new Pipe("pc_count_filtered", new CountBy(priceAssembly, new Fields("adgroup_id1"), new Fields("pc_count")));
				setTails(priceAssembly, rawPCCount, pcCount);
			}else{
				throw new Exception("The priceconf data at hour " + hour + " on " + pDateStr + " does not exist.");
            }            

		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
