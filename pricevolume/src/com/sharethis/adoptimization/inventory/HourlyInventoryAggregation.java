package com.sharethis.adoptimization.inventory;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

public class HourlyInventoryAggregation extends SubAssembly 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyInventoryAggregation.class);
	private static final long serialVersionUID = 1L;

	public HourlyInventoryAggregation(Pipe nbAssembly, String pipeName, String keyFieldStr) throws Exception
	{
		try{
			sLogger.info("Entering HourlyInventoryAggregation ...");
			
			Fields keyFields = new Fields();
			String[] keyFieldArr = StringUtils.split(keyFieldStr, ',');
			for (int i=0; i<keyFieldArr.length; i++){
				keyFields = keyFields.append(new Fields(keyFieldArr[i]));
			}			

			Pipe dataAssembly_nb = null;
			if (!(nbAssembly==null)) {
				String nbPipeName = nbAssembly.getName();
				sLogger.info(nbPipeName + " is not null.");
//				CountBy sum0 = new CountBy(nbAssembly, new Fields("jid"), new Fields("sum_nobids"));
				SumBy sum0 = new SumBy(new Fields("req_cnt"), new Fields("sum_nobids"), Double.class);
				SumBy sum1 = new SumBy(new Fields("bid_price"), new Fields("sum_bp"), Double.class); 
				SumBy sum2 = new SumBy(new Fields("min_bid_price"), new Fields("sum_mbp"), Double.class); 
				dataAssembly_nb = new AggregateBy(pipeName, new Pipe[]{nbAssembly}, keyFields, sum0, sum1, sum2); 
				Function<?> wt_avg = new ComputingWeightedAverage(new Fields("avg_bp", "avg_mbp"));
				dataAssembly_nb = new Each(dataAssembly_nb, new Fields("sum_bp", "sum_mbp", "sum_nobids"), wt_avg, Fields.ALL);			
			}
			
			setTails(dataAssembly_nb);
			sLogger.info("Exiting from HourlyInventoryAggregation.");
  			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
