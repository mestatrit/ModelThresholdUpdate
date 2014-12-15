package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import org.apache.log4j.Logger;

import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

public class HourlyAggregatingPredCTR extends SubAssembly 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyAggregatingPredCTR.class);
	private static final long serialVersionUID = 1L;

	public HourlyAggregatingPredCTR(Pipe ciAssembly, String pipeName, Fields keyFields) throws Exception
	{
		try{
			sLogger.info("Entering HourlyAggregatingCTRData ...");
			SumBy sum1 = new SumBy(new Fields("imp_flag"), new Fields("sum_imps1"), double.class); 
			SumBy sum2 = new SumBy(new Fields("click_flag"), new Fields("sum_clicks1"), double.class); 
			SumBy sum3 = new SumBy(new Fields("cost"), new Fields("sum_cost1"), double.class); 
			SumBy sum4 = new SumBy(new Fields("ctr_pred"), new Fields("sum_clicks_pred1"), double.class); 
			Pipe ctrAssembly = new AggregateBy(pipeName, new Pipe[]{ciAssembly}, keyFields, sum1, sum2, sum3, sum4); 
			Fields allFields = keyFields;
			allFields = allFields.append(new Fields("sum_imps", "sum_clicks", "sum_cost", 
					"sum_clicks_pred", "ctr", "e_cpm", "e_cpc", "ctr_pred"));
			Function<?> ctrFunc = new ComputingPredCTRValue(new Fields("sum_imps", "sum_clicks", 
					"sum_cost", "sum_clicks_pred", "ctr", "e_cpm", "e_cpc", "ctr_pred"));
			ctrAssembly = new Each(ctrAssembly, new Fields("sum_imps1", "sum_clicks1", "sum_cost1", "sum_clicks_pred1"), ctrFunc, allFields);
  			setTails(ctrAssembly);	
			sLogger.info("Exiting from HourlyAggregatingCTRData.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
