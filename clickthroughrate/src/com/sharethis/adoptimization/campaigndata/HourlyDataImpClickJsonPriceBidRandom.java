package com.sharethis.adoptimization.campaigndata;

import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.filter.Not;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.FilterOutDataNEInt;
import com.sharethis.adoptimization.common.FilterOutDataNEStrList;
import com.sharethis.adoptimization.common.RandomlyPickingKeyCategory;
import com.sharethis.adoptimization.common.RandomlyPickingKeyValue;


/**
 * This is the assembly to join the bidder and price confirmation data.
 */

public class HourlyDataImpClickJsonPriceBidRandom extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyDataImpClickJsonPriceBidRandom.class);
	private static final long serialVersionUID = 1L;

	public HourlyDataImpClickJsonPriceBidRandom(Pipe allAssembly, Fields keptFields, Configuration config) 
			throws IOException, ParseException, Exception
	{
		try{				
			Pipe adxAssembly = new Pipe("all_data_adx_desktop", allAssembly);

			Filter<?> filterOutData = new FilterOutDataNEStrList("ADX", "null");
			adxAssembly = new Each(adxAssembly, new Fields("service_type", "deviceType"), filterOutData); 
	
			int uFieldInd = keptFields.getPos("user_list");
			int vFieldInd = keptFields.getPos("vertical_list");

			Function<?> exFunc = new RandomlyPickingKeyValue(keptFields.append(new Fields("usg_id", "vertical_id")), 
					keptFields, uFieldInd, vFieldInd);
			adxAssembly = new Each(adxAssembly, keptFields, exFunc, Fields.RESULTS);	
			
			String modDataClick = config.get("ModDataClick");
			String modDataNonClick = config.get("ModDataNonClick");
			String modDataCount = config.get("ModDataCount");

			Filter<?> filterOut = new FilterOutDataNEInt(1);
			Pipe modSampAssembly_c = new Pipe(modDataClick, new Each(adxAssembly, new Fields("click_flag1"), filterOut)); 				
			Pipe modSampAssembly_nc = new Pipe(modDataNonClick, new Each(adxAssembly, new Fields("click_flag1"), new Not(filterOut))); 
			SumBy sum1 = new SumBy(new Fields("imp_flag1"), new Fields("imp_cnt"), double.class); 
			Pipe cntAssembly = new AggregateBy(modDataCount, new Pipe[]{adxAssembly}, new Fields("click_flag1"), sum1); 
			
		    setTails(adxAssembly, modSampAssembly_c, modSampAssembly_nc, cntAssembly);		    	
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}		
}
