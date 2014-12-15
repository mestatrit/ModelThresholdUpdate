package com.sharethis.adoptimization.pricevolume;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

public class HourlyAggregatingDataBySegment extends SubAssembly 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyAggregatingDataBySegment.class);
	private static final long serialVersionUID = 1L;

	public HourlyAggregatingDataBySegment(Pipe[] allAssembly, String pipeName, 
			String keyFieldStr, String pDateStr) throws Exception
	{
		try{
			sLogger.info("Entering HourlyAggregatingDataBySegment ...");
			
			Fields keyFields = new Fields();
			Fields keyFields1 = new Fields();
			// Aggregating the winning data grouped by the groupFields
			String[] keyFieldArr = StringUtils.split(keyFieldStr, ',');
			for (int i=0; i<keyFieldArr.length; i++){
				keyFields = keyFields.append(new Fields(keyFieldArr[i]));
				keyFields1 = keyFields1.append(new Fields(keyFieldArr[i]+"_1"));
			}
			
			Fields groupFields = keyFields.append(new Fields("winning_price"));
			Fields groupFields1 = keyFields1.append(new Fields("winning_price_1"));

			// Aggregating the successful win data grouped by the groupFields
			Pipe winJoin = new Pipe(pipeName+"_win", new HourlyAggregatingPriceData(allAssembly[0], groupFields, groupFields1, keyFields).getTails()[0]);	

			// Aggregating the successful bid data grouped by the keyFields
			Pipe bidJoin = new Pipe(pipeName+"_bid", new HourlyAggregatingBidData(allAssembly[1], allAssembly[2], keyFields, keyFields1).getTails()[0]);
			
			setTails(winJoin, bidJoin);
			sLogger.info("Exiting from HourlyAggregatingDataBySegment.");
  			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
