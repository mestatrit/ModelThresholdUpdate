package com.sharethis.adoptimization.clickthroughrate;

import org.apache.log4j.Logger;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

public class MappingsHourlySubassembly extends SubAssembly 
{	
	private static final Logger sLogger = Logger.getLogger(MappingsHourlySubassembly.class);
	private static final long serialVersionUID = 1L;

	public MappingsHourlySubassembly(Pipe allAssembly, String mapPipeName, Fields mapKeyFields, String wtField, String dataField) throws Exception
	{
		try{
			sLogger.info("Entering HourlyGeneratingMappings ...");
			SumBy sum1 = new SumBy(new Fields(wtField), new Fields("sum_wt"), double.class); 
			SumBy sum2 = new SumBy(new Fields(dataField), new Fields("sum_imp"), double.class); 
			Pipe mapAssembly = new AggregateBy(mapPipeName, new Pipe[]{allAssembly}, mapKeyFields, sum1, sum2); 
			Fields keptFields = mapKeyFields.append(new Fields("sum_wt", "sum_imp"));
			mapAssembly = new Each(mapAssembly, keptFields, new Identity());				
//			mapAssembly = new Pipe(mapPipeName, new Unique(mapAssembly, mapKeyFields, 100000));
  			setTails(mapAssembly);	
			sLogger.info("Exiting from HourlyGeneratingMappings.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
