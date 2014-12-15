package com.sharethis.adoptimization.pricevolume;

import cascading.operation.Aggregator;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * This is the assembly to join the bidder and price confirmation data.
 */

public class UIDataSubAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(UIDataSubAssembly.class);
	private static final long serialVersionUID = 1L;
	
	public UIDataSubAssembly(Pipe pbAssembly, Pipe modelAssembly, Fields keyFields, Fields keyFields1,
			Fields pFields, Fields mDataFields, int numOfDays, String pDateStr, Configuration config) 
		throws IOException, ParseException, Exception
	{
		try{		
			sLogger.info("Start generating the UI data ...");
			//Joining pAssembly and modelAssembly
			Fields allFields = keyFields;
			allFields = allFields.append(pFields);
			allFields = allFields.append(keyFields1);
			allFields = allFields.append(mDataFields);
			Pipe pbmAssembly = new CoGroup(pbAssembly.getName(), pbAssembly, keyFields, modelAssembly, keyFields, allFields, new LeftJoin());
						
			//Keeping the fields defined in keptFields.
			Fields keptFields = keyFields;
			keptFields = keptFields.append(pFields);
			keptFields = keptFields.append(new Fields("model_type","beta0", "beta1", "max_wp_pred"));
			pbmAssembly = new Each(pbmAssembly, keptFields, new Identity());
			
			Fields uiDataFields = new Fields("winning_price", "sum_wins", "sum_mbids", 
					"cum_wins", "sum_bids", "sum_nobids", "num_days", "date_", "wr_hist", "wr_pred");

			int numOfPointsUI = config.getInt("NumOfPointsUI", 100);
			Aggregator<?> uiData = new UIPredDataGeneration(uiDataFields, numOfPointsUI, numOfDays, pDateStr);
			pbmAssembly = new GroupBy(pbmAssembly, keyFields);
			Pipe uiAssembly = new Every(pbmAssembly, new Fields("winning_price", "sum_wins", "sum_mbids", 
					"cum_wins", "sum_bids", "sum_nobids", "model_type", "beta0", "beta1", "max_wp_pred"), uiData, Fields.ALL); 
			
			setTails(uiAssembly);
			sLogger.info("Generating the UI data is done.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}	
}
