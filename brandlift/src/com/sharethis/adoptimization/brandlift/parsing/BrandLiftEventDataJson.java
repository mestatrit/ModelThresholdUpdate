package com.sharethis.adoptimization.brandlift.parsing;

import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.filter.Not;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Unique;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.BLConstants;
import com.sharethis.adoptimization.common.FilterOutNullData;


/**
 * This is the assembly to read retarget data.
 */

public class BrandLiftEventDataJson extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(BrandLiftEventDataJson.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public BrandLiftEventDataJson(Map<String, Tap> sources, String filePath, String pDateStr, 
			int hour, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{		
			// Defining the fields of retarget data
            Fields retargFields = new Fields("retarg_cookie", "retargJson");	
			Class[] types = new Class[]{String.class, String.class};
			
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			String hourFolder = BLConstants.hourFolders[hour];
			// Building the taps from the data files
			Pipe retargAssembly = null;
			String filePathHourly = filePath + dayStr + hourFolder + "/data/";
            
			Scheme retargSourceScheme = new TextDelimited(retargFields, false, mDelimiter, types);
			Tap retargTap = new Hfs(retargSourceScheme, filePathHourly);
			JobConf	jobConf = new JobConf();
			if(retargTap.resourceExists(jobConf)){
				sLogger.info(filePathHourly + " is used.");
				String pipeName = "bl_parsed";
				retargAssembly = new Pipe(pipeName);
				sources.put(pipeName, retargTap);
			}else{
				sLogger.info("The data path: " + filePathHourly + " does not exist.");
			}
            
            Fields jsonField = new Fields("retargJson");
	    	retargAssembly = new Each(retargAssembly, jsonField, new Identity());
		
            //Parsing json format to the data fields for the pipe.
	    	Fields outFields = new Fields(BLConstants.retargNames);
	    	outFields = outFields.append(new Fields(BLConstants.blNames));
            Function<?> jsonParsing = new ParsingJsonFormatBLData(outFields, 
            		BLConstants.retargJsonNames, BLConstants.blNames);
			retargAssembly = new Each(retargAssembly, jsonField, jsonParsing, Fields.RESULTS); 

			Filter<?> filterOutNull = new FilterOutNullData();
			retargAssembly = new Each(retargAssembly, new Fields("retarg_jid"), filterOutNull); 

			if(retargAssembly != null){
				retargAssembly = new Unique(retargAssembly, new Fields("retarg_jid"), 100000);
			}

			setTails(retargAssembly);            
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}

}
