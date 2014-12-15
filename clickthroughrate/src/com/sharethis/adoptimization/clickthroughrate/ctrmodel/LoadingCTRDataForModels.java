package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * This is the assembly to read ctr data.
 */

public class LoadingCTRDataForModels extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(LoadingCTRDataForModels.class);
	private static final long serialVersionUID = 1L;
	private static String mDelimiter = "\t";
		
	public LoadingCTRDataForModels(Map<String, Tap> sources, String ctrFilePath, 
			String pDateStr, Configuration config, String pipeName, 
			Fields dFields, Class[] types, String infile_postfix) 
		throws IOException, ParseException, Exception
	{
		try{		
            Scheme dSourceScheme = new TextDelimited(dFields, false, mDelimiter, types);
            Pipe dAssembly = new Pipe(pipeName);
            List<Tap> pbTapL = new ArrayList<Tap>();
        	String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			String filePath = ctrFilePath + dayStr + "/" + pipeName + infile_postfix;

           	// Building the taps from the data files
            Tap cTap = new Hfs(dSourceScheme, filePath);
    		JobConf	jobConf = new JobConf();
    		if(cTap.resourceExists(jobConf)){
    			sLogger.info(filePath + " is used.");
				sources.put(pipeName, cTap);
				setTails(dAssembly);            
    		}else{
    			sLogger.info("The data path: " + filePath + " does not exist.");				    				
            	setTails(new Pipe("No_Data", null));
    		}    				
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
