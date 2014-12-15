package com.sharethis.adoptimization.pricevolume;

import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.FilterOutNullData;

import cascading.operation.Aggregator;
import cascading.operation.Filter;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.filter.Not;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;

public class HourlyAggregatingPriceData extends SubAssembly 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyAggregatingPriceData.class);
	private static final long serialVersionUID = 1L;

	public HourlyAggregatingPriceData(Pipe pAssembly, Fields groupFields, Fields groupFields1, Fields keyFields) throws Exception
	{
		try{
			sLogger.info("Entering HourlyAggregatingPriceData ...");

			String pPipeName = pAssembly.getName();	

			Pipe winJoin = null;
			if (!pAssembly.equals(null)) {
				Pipe winAssembly = null;
				Pipe winAssembly_mbid = null;
				sLogger.info(pPipeName + " is not null.");
				Aggregator<?> cnt = new Count(new Fields("sum_wins"));
				winAssembly = new GroupBy(pPipeName, pAssembly, groupFields);			
				winAssembly = new Every(winAssembly, cnt);
				
				Filter<?> filter = new Not(new FilterOutNullData());
				winAssembly_mbid = new Each(pAssembly, new Fields("jid"), filter);
				Aggregator<?> cnt1 = new Count(new Fields("sum_mbids"));
				winAssembly_mbid = new GroupBy(pPipeName+"_1", winAssembly_mbid, groupFields);			
				winAssembly_mbid = new Every(winAssembly_mbid, cnt1);

				Fields allFields = new Fields();
				allFields = allFields.append(groupFields);
				allFields = allFields.append(new Fields("sum_wins"));
				allFields = allFields.append(groupFields1);
				allFields = allFields.append(new Fields("sum_mbids"));
				// Joining winAssembly and winAssembly_mbid
				winJoin = new CoGroup(winAssembly, groupFields, winAssembly_mbid, groupFields, allFields, new LeftJoin());
				
				Fields keptFields = groupFields;
				keptFields = keptFields.append(new Fields("sum_wins","sum_mbids"));
				// Keeping the fields defined in keptFields.
				winJoin = new Each(winJoin, keptFields, new Identity());
//				Fields sortField = new Fields("winning_price"); 
//				winJoin = new GroupBy(winJoin, keyFields, sortField); 
//				Buffer<?> accum = new RunningCount(new Fields("cum_wins"));
//				winJoin = new Every(winJoin, new Fields("sum_wins"), accum, Fields.ALL); 
				
			}else{
				sLogger.info(pPipeName + " is null.");
			}							
			
  			setTails(winJoin);	
			sLogger.info("Exiting from HourlyAggregatingPriceData.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
