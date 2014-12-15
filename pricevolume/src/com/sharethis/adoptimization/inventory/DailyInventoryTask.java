package com.sharethis.adoptimization.inventory;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DailyInventoryTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(DailyInventoryTask.class);
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public int run(String[] args) throws Exception {
		sLogger.info("Getting the configuration ...");
		Configuration config = ConfigurationUtil.setConf(args);
		sLogger.info("Getting the configuration is done.");	
		
		if(config.get("ProcessingDate")==null){
			// If ProcessDate is null, then using the date of yesterday as the processing date.
			Date date = DateUtils.addDays(new Date(), -1);
			config.set("ProcessingDate", sdf.format(date));
		}

		String pDateStr = config.get("ProcessingDate");
		DailyInventoryGeneration dailyNB = new DailyInventoryGeneration();
		String basePipeName = config.get("BasePipeNameDaily");
		String baseKeyFields = config.get("BaseKeyFieldsDaily");
		String pipeNamesList = config.get("PipeNamesListDaily");
		String keyFieldsList = config.get("KeyFieldsListDaily");
		String infile_postfix = "_hourly";
		String outfile_postfix = "_daily";
		if(pipeNamesList!=null){
			sLogger.info("\nGenerating the daily inventory data on " + pDateStr + " ...");
			dailyNB.generatingInventoryDaily(config, basePipeName, baseKeyFields, pipeNamesList, 
					keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("Generating the daily inventory on " + pDateStr + " is done.");
		}
		return 0;	
	}		
}
