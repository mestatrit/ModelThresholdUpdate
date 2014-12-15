package com.sharethis.adoptimization.inventory;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * This is the assembly to append the daily data over the number of days.
 */

public class PeriodicalInventorySubAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(PeriodicalInventorySubAssembly.class);
	private static final long serialVersionUID = 1L;
	
	public PeriodicalInventorySubAssembly(){}
	
	public PeriodicalInventorySubAssembly(Map<String, Tap> sources, String outFilePath, String pDateStr, 
			int numOfPeriods, int interval, Configuration config, String pipeNameList, 
			String keyFieldsList, String basePipeName, String baseKeyFieldsStr, int hour, String file_postfix) 
		throws IOException, ParseException, Exception
	{
		try{		
			String[] keyFieldBaseArr = StringUtils.split(baseKeyFieldsStr,",");
			// Defining the kept fields after price and bidder are merged.

			Fields bDataFields = new Fields("sum_nobids", "sum_bp", "sum_mbp", "avg_bp", "avg_mbp");
			Fields bFields = new Fields();
			int baseKeyLen = keyFieldBaseArr.length;
			Class[] bTypes = new Class[baseKeyLen+bDataFields.size()];
			Fields baseKeyFields = new Fields();
			for (int i=0; i<baseKeyLen; i++){
				bTypes[i] = String.class;
				baseKeyFields = baseKeyFields.append(new Fields(keyFieldBaseArr[i]));
			}
			bFields = baseKeyFields.append(bDataFields);
			sLogger.info("bFields: " + bFields);
            for(int i=0; i<bDataFields.size(); i++)
            	bTypes[baseKeyLen+i] = double.class;
			
			String[] pipeNames = StringUtils.split(pipeNameList,";");
			int pipeLen = pipeNames.length;
			Pipe[] bAssembly = new Pipe[pipeLen+1];

			Pipe[] dataAssembly = new PeriodicalInventory(sources, outFilePath, pDateStr, numOfPeriods, interval, config, 
					bFields, basePipeName, bTypes, hour, file_postfix).getTails();
			Pipe bAssembly_tmp = new AggregatingInventory(dataAssembly, basePipeName+"_tmp", baseKeyFields).getTails()[0];
			bAssembly[pipeLen] = new Pipe(basePipeName, bAssembly_tmp);
			String[] keyFieldStr = StringUtils.split(keyFieldsList, ";");
			Fields keyFields = null;
			for(int i_p=0; i_p<pipeLen; i_p++){
				String[] keyFieldArr = StringUtils.split(keyFieldStr[i_p], ',');
				keyFields= new Fields();
				for (int i=0; i<keyFieldArr.length; i++){
					keyFields = keyFields.append(new Fields(keyFieldArr[i]));
				}		

				String bPipeName = pipeNames[i_p];
				bAssembly[i_p] = new AggregatingInventory(new Pipe[]{bAssembly_tmp}, bPipeName, keyFields).getTails()[0];
			}
            
			setTails(bAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
