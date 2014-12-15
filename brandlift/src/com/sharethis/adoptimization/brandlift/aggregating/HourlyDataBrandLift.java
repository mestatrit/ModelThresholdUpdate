package com.sharethis.adoptimization.brandlift.aggregating;

import cascading.operation.Filter;
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

import com.sharethis.adoptimization.common.BLConstants;
import com.sharethis.adoptimization.common.FilterOutNullData;


/**
 * This is the assembly to read brand lift data.
 */

public class HourlyDataBrandLift extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataBrandLift.class);
	private static final long serialVersionUID = 1L;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static String mDelimiter = "\t";
	
	public HourlyDataBrandLift(Map<String, Tap> sources, String blFilePath, String blFileName, 
			String pDateStr, int hour, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{		
			
			// Defining the fields of brand lift data
            Fields blFields = new Fields("retarg_type","retarg_cookie","retarg_campaign_name",
            		"retarg_user_agent","retarg_date","retarg_url","retarg_rttype",
             		"retarg_ip","retarg_zip","retarg_city","retarg_st","retarg_lon","retarg_lan",
             		"retarg_dma","retarg_ctry", "pid","vzcid","adtype","vzadid","ttype","vzsid",
             		"siteurl","retarg_jid","visible","_t","preexp","postexp","impcnt","uid","aid","t");	
            
            Class[] types = new Class[]{String.class, String.class, String.class, 
            		String.class, String.class, String.class, String.class, 
            		String.class, String.class, String.class, String.class, String.class, String.class,
            		String.class, String.class, String.class, String.class, 
            		String.class, String.class, String.class, String.class, 
            		String.class, String.class, String.class, String.class, String.class, 
            		String.class, String.class, String.class, String.class, String.class 
            		};           
            
            String hourFolder = BLConstants.hourFolders[hour]; 
            String pipeName = "blSource_" + hourFolder;

    		Date pDate = sdf.parse(pDateStr);
            // Building the taps from the data files
			Scheme blSourceScheme = new TextDelimited(blFields, null, false, false, mDelimiter, false, "", types, true);
            List<Tap> blTapL = new ArrayList<Tap>();
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

				// The change is to handle the new data path: yyyyMMddhh
				String filePathHourly = blFilePath + dayStr_temp + BLConstants.hourFolders[i_temp] + "/" + blFileName + "/";

				// Building the taps from the data files
            	Tap cTap = new Hfs(blSourceScheme, filePathHourly);
    			JobConf	jobConf = new JobConf(config);
    			if(cTap.resourceExists(jobConf)){
    				sLogger.info(filePathHourly + " is used.");
    				blTapL.add(ind, cTap);
    				ind++;
    			}else{
    				sLogger.info("The data path: " + filePathHourly + " does not exist.");				    				
    			}    				
            }	
            int tapLen = blTapL.size();
            Pipe blAssembly = null;
            if(tapLen>0){
            	blAssembly = new Pipe(pipeName);
            	Tap[] priceTap = new Tap[tapLen];
            	for(int i=0; i<tapLen; i++)
            		priceTap[i] = blTapL.get(i);
            	// Defining source Taps
				MultiSourceTap cSources = new MultiSourceTap(priceTap);
				sources.put(pipeName, cSources);
				
				Filter<?> filterOutNull = new FilterOutNullData();
				blAssembly = new Each(blAssembly, new Fields("retarg_jid"), filterOutNull); 
				blAssembly = new Unique(blAssembly, new Fields("retarg_jid"), 100000);
				
				setTails(blAssembly);
			}else{
				throw new Exception("The brand lift data at hour " + hour + " on " + pDateStr + " does not exist.");
            }            

		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
