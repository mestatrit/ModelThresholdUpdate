package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import cascading.operation.Aggregator;
import cascading.operation.Buffer;
import cascading.operation.Filter;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.joiner.LeftJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.FilterOutNullData;
import com.sharethis.adoptimization.common.RunningCounts;


/**
 * This is the assembly to read price confirmation data.
 */

public class PCProcessingModel extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(PCProcessingModel.class);
	private static final long serialVersionUID = 1L;
	
	public PCProcessingModel(Map<String, Tap> sources, Configuration config, String pDateStr, 
			String ctrFilePath, String modFilePath, String modPipeName, String modKeyFields, 
			String infile_postfix) throws Exception{
		try{
			sLogger.info("\nStarting to process the ctr data for PC models on " + pDateStr + " ...");			
			Fields dataFields = new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc");
			
			//Loading the data for price-click model.
	    	String[] keyFieldsStr = StringUtils.split(modKeyFields, ",");
	    	Fields keyFields = new Fields();
	    	Fields pKeyFields = new Fields();
	    	Fields pKeyFields_1 = new Fields();
	    	for(int j=0; j<keyFieldsStr.length;j++){
	    		if(!keyFieldsStr[j].equalsIgnoreCase("wp_bucket")){
	    			pKeyFields = pKeyFields.append(new Fields(keyFieldsStr[j]));
	    			pKeyFields_1 = pKeyFields_1.append(new Fields(keyFieldsStr[j]+"_1"));
	    		}
	    		keyFields = keyFields.append(new Fields(keyFieldsStr[j]));
	    	}
	    	Fields yFields = keyFields.append(dataFields);
	    	Class[] yTypes = new Class[yFields.size()];
	    	for(int j=0; j<keyFieldsStr.length;j++){
	    		yTypes[j] = String.class;
	    	}
	    	for(int j=keyFieldsStr.length; j<yFields.size()-1; j++){
	    		yTypes[j] = Double.class;
	    	}
	    	yTypes[yFields.size()-1] = String.class;
	    	
	    	Pipe pcAssembly = new LoadingCTRDataForModels(sources, ctrFilePath, pDateStr, 
	    			config, modPipeName, yFields, yTypes, infile_postfix).getTails()[0];

	    	// Filtering out the data with 'null' wp_bucket
			Filter<?> filterOutNull = new FilterOutNullData();
			pcAssembly = new Each(pcAssembly, new Fields("wp_bucket"), filterOutNull); 

			//Computing the total values for each group
			SumBy sum1 = new SumBy(new Fields("sum_imps"), new Fields("total_imps"), double.class); 
			SumBy sum2 = new SumBy(new Fields("sum_clicks"), new Fields("total_clicks"), double.class); 
			SumBy sum3 = new SumBy(new Fields("sum_cost"), new Fields("total_cost"), double.class); 
			Pipe aggAssembly = new AggregateBy(modPipeName+"_agg", new Pipe[]{pcAssembly}, pKeyFields, sum1, sum2, sum3); 

			// Building the pipe to join the total data
			Fields allFields = yFields;
			allFields = allFields.append(pKeyFields_1);
			allFields = allFields.append(new Fields("total_imps", "total_clicks", "total_cost"));
	    	pcAssembly = new CoGroup(modPipeName, pcAssembly, pKeyFields, aggAssembly, pKeyFields, allFields, new LeftJoin());
			
	    	//Keeping the ketpFields
	    	Fields keptFields = keyFields;
	    	keptFields = keptFields.append(new Fields("e_cpm", "sum_clicks", "sum_imps", "total_imps"));
	    	pcAssembly = new Each(pcAssembly, keptFields, new Identity());
	    	
			// The 'head' of the accumulative assembly
			String cumPipeName = modPipeName+"_cum";
			Pipe pcAssemblyCum = new Pipe(cumPipeName, pcAssembly);

			// Accumulating the sum_imps, sum_clicks and sum_cost for all price buckets.												
			Fields sortField = new Fields("wp_bucket"); 		
			pcAssemblyCum = new GroupBy(cumPipeName, pcAssemblyCum, pKeyFields, sortField); 
			Buffer<?> accum = new RunningCounts(new Fields("cum_clicks"), 1);
			pcAssemblyCum= new Every(pcAssemblyCum, new Fields("sum_clicks"), accum, Fields.ALL); 

	    	
			int numOfPoints = config.getInt("NumOfPoints", 10);
			double rSquareVal = config.getFloat("RSquareVal", 0.0001f);
			double maxPredCTR = config.getFloat("MaxPredCTR", 0.75f);
			int numOfDays = 84;
			
			//TODO: Filtering out the data with sum_imp < Threshold_Value for this type of models
			Pipe modelAssembly = new Pipe("pc_"+modPipeName+"_model", pcAssemblyCum);
			Aggregator<?> baseModel = new PCModelRegression(new Fields("model_type", "beta0", "beta1", 
						"r_square", "num_wp", "ctr", "min_wp", "max_wp", "max_wp_pred", "num_days", "date_"), 
						numOfPoints, rSquareVal, maxPredCTR, numOfDays, pDateStr);
			modelAssembly = new GroupBy(modelAssembly, pKeyFields);
			modelAssembly = new Every(modelAssembly, new Fields("e_cpm", "sum_clicks", "sum_imps"), baseModel, Fields.ALL); 
			
			Pipe modelAssemblyCum = new Pipe("pc_"+cumPipeName+"_model", pcAssemblyCum);
			Aggregator<?> baseModelCum = new PCModelRegression(new Fields("model_type", "beta0", "beta1", 
						"r_square", "num_wp", "ctr", "min_wp", "max_wp", "max_wp_pred", "num_days", "date_"), 
						numOfPoints, rSquareVal, maxPredCTR, numOfDays, pDateStr);
			modelAssemblyCum = new GroupBy(modelAssemblyCum, pKeyFields);
			modelAssemblyCum = new Every(modelAssemblyCum, new Fields("e_cpm", "cum_clicks", "total_imps"), baseModelCum, Fields.ALL); 

			sLogger.info("Processing the ctr data for PC models on " + pDateStr + " is done.\n");			
			setTails(new Pipe[]{pcAssemblyCum, modelAssembly, modelAssemblyCum});	
		
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
}
