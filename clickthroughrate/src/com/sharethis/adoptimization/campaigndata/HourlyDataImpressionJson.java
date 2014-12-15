package com.sharethis.adoptimization.campaigndata;

import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Unique;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.CTRConstants;
import com.sharethis.adoptimization.common.ParsingJsonFormatImpData;


/**
 * This is the assembly to read bidder data.
 */

public class HourlyDataImpressionJson extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataImpressionJson.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
	
	public HourlyDataImpressionJson(Map<String, Tap> sources, String filePath, String pDateStr, int hour, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{						
			// Defining the fields of bidder data
			Fields impFields = new Fields("cookie_id", "impJson");
			//Fields impFields = new Fields("impJsons");
			Class[] types = new Class[]{String.class, String.class};
			
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			String hourFolder = CTRConstants.hourFolders[hour];
			// Building the taps from the data files
			Pipe impAssembly = null;
//			String filePathHourly = filePath + dayStr + "/" + hourFolder + "/";
			String filePathHourly = filePath + dayStr + hourFolder + "/data/";
            
			//Scheme impSourceScheme = new TextDelimited(impFields);
			Scheme impSourceScheme = new TextDelimited(impFields, false, mDelimiter, types);
            //Scheme impSourceScheme = new TextLine(impFields);
			Tap impTap = new Hfs(impSourceScheme, filePathHourly);
			JobConf	jobConf = new JobConf();
			if(impTap.resourceExists(jobConf)){
				sLogger.info(filePathHourly + " is used.");
				String pipeName = "imp_" + hourFolder;
				impAssembly = new Pipe(pipeName);
				sources.put(pipeName, impTap);
			}else{
				sLogger.info("The data path: " + filePathHourly + " does not exist.");
			}

			Fields impJsonField = new Fields("impJson");
	    	impAssembly = new Each(impAssembly, impJsonField, new Identity());
		
            Function<?> jsonParsing = new ParsingJsonFormatImpData(new Fields(CTRConstants.impNames), CTRConstants.impJsonNames);
			impAssembly = new Each(impAssembly, impJsonField, jsonParsing, Fields.RESULTS); 
			
			if(impAssembly != null)
				impAssembly = new Unique(impAssembly, new Fields("jid"), 100000);

			setTails(impAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
