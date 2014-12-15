package com.sharethis.adoptimization.pricevolume;

import org.apache.log4j.Logger;

import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;

public class HourlyAggregatingBidData extends SubAssembly 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyAggregatingBidData.class);
	private static final long serialVersionUID = 1L;

	public HourlyAggregatingBidData(Pipe bAssembly, Pipe nbAssembly, Fields keyFields, Fields keyFields1) throws Exception
	{
		try{
			sLogger.info("Entering HourlyAggregatingBidData ...");

			// Aggregating the successful bid data grouped by the groupFields
			Pipe bidAssembly = null;
			if (!(bAssembly==null)) {
				String bPipeName = bAssembly.getName();
				sLogger.info(bPipeName + " is not null.");
				Aggregator<?> cnt = new Count(new Fields("sum_bids"));
				bidAssembly = new GroupBy(bPipeName, bAssembly, keyFields);						
				bidAssembly = new Every(bidAssembly, new Fields("jid"), cnt);
			}

			Pipe bidAssembly_nb = null;
			if (!(nbAssembly==null)) {
				String nbPipeName = nbAssembly.getName();
				sLogger.info(nbPipeName + " is not null.");
				Aggregator<?> cnt = new Count(new Fields("sum_nobids"));
				bidAssembly_nb = new GroupBy(nbPipeName, nbAssembly, keyFields);						
				bidAssembly_nb = new Every(bidAssembly_nb, new Fields("jid"), cnt);
			}
			
			Pipe bidJoin = null;
			if(!(bidAssembly_nb==null)){
				Fields allFields = new Fields();
				allFields = allFields.append(keyFields);
				allFields = allFields.append(new Fields("sum_bids"));
				allFields = allFields.append(keyFields1);
				allFields = allFields.append(new Fields("sum_nobids"));
				// Joining bidAssembly and bidAssembly_nb
				bidJoin = new CoGroup(bidAssembly, keyFields, bidAssembly_nb, keyFields, allFields, new LeftJoin());			
				Fields keptFields = keyFields;
				keptFields = keptFields.append(new Fields("sum_bids","sum_nobids"));
				// Keeping the fields defined in keptFields.
				bidJoin = new Each(bidJoin, keptFields, new Identity());
			}else{
				Function<?> aValue = new AssignDummyValue(new Fields("sum_nobids"), 0);
				bidJoin = new Each(bidAssembly, aValue, Fields.ALL);							
			}
									
  			setTails(bidJoin);	
			sLogger.info("Exiting from HourlyAggregatingBidData.");
  			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
