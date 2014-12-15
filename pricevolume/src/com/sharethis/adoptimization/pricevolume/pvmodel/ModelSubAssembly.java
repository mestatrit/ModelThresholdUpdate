package com.sharethis.adoptimization.pricevolume.pvmodel;

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
 * This is the assembly to append the daily data over the number of days.
 */

public class ModelSubAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(ModelSubAssembly.class);
	private static final long serialVersionUID = 1L;
	
	public ModelSubAssembly(Pipe wAssembly, Pipe bAssembly, String pipeName, Fields keyFields, Fields keyFields1, 
			Configuration config, int numOfPoints, double rSquareVal, double maxPredWR, int numOfDays, String pDateStr) 
		throws IOException, ParseException, Exception
	{
		try{		           
			sLogger.info("Start generating the predictive models ...");
			Fields pDataFields = new Fields("winning_price","sum_wins","sum_mbids","cum_wins");
			Fields bDataFields = new Fields("sum_bids","sum_nobids");
 			
			Pipe dataAssembly = null;
			Pipe modelAssembly = null;
	
			sLogger.info(wAssembly.getName() + "    " + bAssembly.getName() + "    " + keyFields.toString());
			Fields allFields = keyFields;
			allFields = allFields.append(pDataFields);
			allFields = allFields.append(keyFields1);
			allFields = allFields.append(bDataFields);
			Fields keptFields = keyFields;
			keptFields = keptFields.append(new Fields("winning_price", "sum_wins", "cum_wins", "sum_bids"));
			sLogger.info("Assemblys:  " + wAssembly.getName() + "    " + bAssembly.getName() + "    " + keyFields.toString());
			dataAssembly = new CoGroup(pipeName+"_data", wAssembly, keyFields, bAssembly, keyFields, allFields, new LeftJoin());		
			dataAssembly = new Each(dataAssembly, keptFields, new Identity());	
				
			modelAssembly = new Pipe(pipeName+"_model_p", dataAssembly);
			Aggregator<?> baseModel = new ModelRegressionBase(new Fields("model_type", "beta0", "beta1", 
						"r_square", "num_wp", "win_rate",  "min_wp", "max_wp", "max_wp_pred", "num_days", "date_"), 
						numOfPoints, rSquareVal, maxPredWR, numOfDays, pDateStr);
			modelAssembly = new GroupBy(modelAssembly, keyFields);
			modelAssembly = new Every(modelAssembly, new Fields("winning_price","cum_wins", "sum_bids"), baseModel, Fields.ALL); 
									
			setTails(modelAssembly, dataAssembly);
			sLogger.info("Generating the predictive models is done.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
