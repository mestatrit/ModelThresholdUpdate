package com.sharethis.adoptimization.inventory;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
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

public class DailyInventorySubAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(DailyInventorySubAssembly.class);
	private static final long serialVersionUID = 1L;
	
	public DailyInventorySubAssembly(){}
	
	public DailyInventorySubAssembly(Map<String, Tap> sources, String outFilePath, String pDateStr, 
			String[] hourFolders, Configuration config, String pipeNameList, String keyFieldsList,
			String pipeNameHour, String keyFieldsHour) 
		throws IOException, ParseException, Exception
	{
		try{		
			String[] keyFieldHourArr = StringUtils.split(keyFieldsHour,",");

			Fields bDataFields = new Fields("sum_nobids", "sum_bp", "sum_mbp", "avg_bp", "avg_mbp");
			Fields bFields = new Fields();
			int hourKeyLen = keyFieldHourArr.length;
			Class[] bTypes = new Class[hourKeyLen+bDataFields.size()];
			Fields hourKeyFields = new Fields();
			for (int i=0; i<hourKeyLen; i++){
				bTypes[i] = String.class;
				hourKeyFields = hourKeyFields.append(new Fields(keyFieldHourArr[i]));
			}
			bFields = hourKeyFields.append(bDataFields);
			sLogger.info("bFields: " + bFields);
            for(int i=0; i<bDataFields.size(); i++)
            	bTypes[hourKeyLen+i] = double.class;
			
			String[] pipeNames = StringUtils.split(pipeNameList,";");
			int pipeLen = pipeNames.length;
			Pipe[] bAssembly = new Pipe[pipeLen+1];

			Pipe[] dataAssembly = new DailyInventory(sources, outFilePath, pDateStr, hourFolders, config, 
					bFields, pipeNameHour, bTypes).getTails();

			Pipe bAssembly_tmp = new AggregatingInventory(dataAssembly, pipeNameHour+"_tmp", hourKeyFields).getTails()[0];

			bAssembly[pipeLen] = new Pipe(pipeNameHour, bAssembly_tmp);
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
