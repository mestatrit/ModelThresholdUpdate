package com.sharethis.adoptimization.pricevolume.pvmodel;

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

public class ModelBaseModelSubAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(ModelBaseModelSubAssembly.class);
	private static final long serialVersionUID = 1L;
	
	public ModelBaseModelSubAssembly(Map<String, Tap> sources, String outFilePath, String pDateStr, 
			Configuration config, String modelNameList, String modelFieldsList, 
			int numOfPoints, double rSquareVal, int numOfDays, double maxPredWR) 
		throws IOException, ParseException, Exception
	{
		try{		
			// Defining the kept fields after price and bidder are merged.
			String[] pipeNames = StringUtils.split(modelNameList,";");
			int pipeLen = pipeNames.length;
			String[] keyFieldStr = StringUtils.split(modelFieldsList, ";");
			Fields[] keyFields = new Fields[pipeLen];
            Fields[] keyFields1 = new Fields[pipeLen];
			Fields[] pFields = new Fields[pipeLen];
			Fields[] bFields = new Fields[pipeLen];
            Class[][] pTypes = new Class[pipeLen][];
            Class[][] bTypes = new Class[pipeLen][];
           
			Fields pDataFields = new Fields("winning_price","sum_wins","sum_mbids","cum_wins");
			Fields bDataFields = new Fields("sum_bids","sum_nobids");
			if(numOfDays==7){
				pDataFields = new Fields("winning_price","sum_wins","sum_mbids","cum_wins","num_days","date_");
				bDataFields = new Fields("sum_bids","sum_nobids", "num_days_1", "date_1");
			}
			for(int i_p=0; i_p<pipeLen; i_p++){
				String[] keyFieldArr = StringUtils.split(keyFieldStr[i_p], ',');
				keyFields[i_p] = new Fields();
				keyFields1[i_p] = new Fields();
				pTypes[i_p] = new Class[keyFieldArr.length+pDataFields.size()];
				bTypes[i_p] = new Class[keyFieldArr.length+bDataFields.size()];
				for (int i=0; i<keyFieldArr.length; i++){
					keyFields[i_p] = keyFields[i_p].append(new Fields(keyFieldArr[i]));
					keyFields1[i_p] = keyFields1[i_p].append(new Fields(keyFieldArr[i]+"_1"));
					pTypes[i_p][i] = String.class;
					bTypes[i_p][i] = String.class;
				}
				pFields[i_p] = keyFields[i_p].append(pDataFields);
				bFields[i_p] = keyFields1[i_p].append(bDataFields);
				sLogger.info("i_p: " + i_p + "     pFields: " + pFields[i_p] + "    i_p: " 
						+ i_p + "     bFields: " + bFields[i_p]);
            	pTypes[i_p][keyFieldArr.length] = double.class;
                for(int i=1; i<pDataFields.size()-1; i++)
                	pTypes[i_p][keyFieldArr.length+i] = int.class;
                for(int i=0; i<bDataFields.size()-1; i++)
                	bTypes[i_p][keyFieldArr.length+i] = int.class;
                
                pTypes[i_p][keyFieldArr.length+pDataFields.size()-1] = String.class;
                bTypes[i_p][keyFieldArr.length+bDataFields.size()-1] = String.class;
			}
			
			Pipe[] pAssembly = new ModelDataWinSubAssembly(sources, outFilePath, pDateStr, config, 
					keyFields, pFields, pipeNames, pTypes, numOfDays).getTails(); 
			
			Pipe[] bAssembly = new ModelDataBidSubAssembly(sources, outFilePath, pDateStr, config, 
					keyFields1, bFields, pipeNames, bTypes, numOfDays).getTails();
 			
			Pipe[] mAssembly = new Pipe[pAssembly.length];
			for(int i=0; i<pAssembly.length; i++){
				Pipe pAssembly_tmp = new Pipe(pAssembly[i].getName()+"_"+i, pAssembly[i]);
				Pipe bAssembly_tmp = new Pipe(bAssembly[i].getName()+"_"+i, bAssembly[i]);
				mAssembly[i] = new ModelSubAssembly(pAssembly_tmp, bAssembly_tmp, pipeNames[i], keyFields[i], 
						keyFields1[i], config, numOfPoints, rSquareVal, maxPredWR, numOfDays, pDateStr).getTails()[0];
			}
			
			setTails(mAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
