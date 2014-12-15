package com.sharethis.adoptimization.pricevolume;

import cascading.operation.Buffer;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.FilterOutDataGEInt;
import com.sharethis.adoptimization.common.OrderingCount;
import com.sharethis.adoptimization.pricevolume.pvmodel.ModelSubAssembly;

/**
 * This is the assembly to append the daily data over the number of days.
 */

public class HourlyAppendingData extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyAppendingData.class);
	private static final long serialVersionUID = 1L;
	
	public HourlyAppendingData(){}
	
	public HourlyAppendingData(Map<String, Tap> sources, String outFilePath, String pDateStr, 
			String[] hourFolders, Configuration config, String pipeNameList, String keyFieldsList,
			String pipeNameHour, String keyFieldsHour, String bidThValHour, String bidThVal) 
		throws IOException, ParseException, Exception
	{
		try{		
			String[] keyFieldHourArr = StringUtils.split(keyFieldsHour,",");
			// Defining the kept fields after price and bidder are merged.

			Fields pDataFields = new Fields("winning_price","sum_wins","sum_mbids");
			Fields bDataFields = new Fields("sum_bids","sum_nobids");
			Fields pFields = new Fields();
			Fields bFields = new Fields();
			int hourKeyLen = keyFieldHourArr.length;
			Class[] pTypes = new Class[hourKeyLen+pDataFields.size()];
			Class[] bTypes = new Class[hourKeyLen+bDataFields.size()];
			Fields hourKeyFields = new Fields();
			Fields hourKeyFields1 = new Fields();
			for (int i=0; i<hourKeyLen; i++){
				pTypes[i] = String.class;
				bTypes[i] = String.class;
				hourKeyFields = hourKeyFields.append(new Fields(keyFieldHourArr[i]));
				hourKeyFields1 = hourKeyFields1.append(new Fields(keyFieldHourArr[i]+"_1"));
			}
			pFields = hourKeyFields.append(pDataFields);
			bFields = hourKeyFields.append(bDataFields);
			sLogger.info("pFields: " + pFields + "     bFields: " + bFields);
        	pTypes[hourKeyLen] = double.class;
            for(int i=1; i<pDataFields.size(); i++)
            	pTypes[hourKeyLen+i] = int.class;
            for(int i=0; i<bDataFields.size(); i++)
            	bTypes[hourKeyLen+i] = int.class;
			
			Pipe pAssemblyBase = new HourlyAppendingPriceDataBase(sources, outFilePath, pDateStr, hourFolders, config, 
					hourKeyFields, pFields, pipeNameHour, pTypes).getTails()[0];

			Pipe bAssemblyBase = new HourlyAppendingBidDataBase(sources, outFilePath, pDateStr, hourFolders, config, 
					hourKeyFields, bFields, pipeNameHour, bTypes).getTails()[0];

			Pipe pAssemblyBase_tmp = new Pipe(pAssemblyBase.getName()+"_tmp", pAssemblyBase);
			Pipe bAssemblyBase_tmp = new Pipe(bAssemblyBase.getName()+"_tmp", bAssemblyBase);

			int numOfPoints = config.getInt("NumOfPoints", 15);
	        double rSquareVal = config.getFloat("RSquareVal", 0.90f);
	        double maxPredWR = config.getFloat("MaxPredWR", 0.75f);
			Fields modelDataFields = new Fields("model_type", "beta0", "beta1", 
					"r_square", "num_wp", "win_rate",  "min_wp", "max_wp", "max_wp_pred", "num_days", "date_");

			Fields dataFields = pDataFields.append(new Fields("cum_wins"));
			dataFields = dataFields.append(bDataFields);
			
			Pipe[] mAssemblyBase = new ModelSubAssembly(pAssemblyBase_tmp, bAssemblyBase_tmp, pipeNameHour, hourKeyFields, 
					hourKeyFields1, config, numOfPoints, rSquareVal, maxPredWR, 1, pDateStr).getTails();
			
			//String[] bidThVals = StringUtils.split(bidThVal, ";");
			String[] pipeNames = StringUtils.split(pipeNameList,";");
			int pipeLen = pipeNames.length;
			String[] keyFieldStr = StringUtils.split(keyFieldsList, ";");
			Fields[] keyFields = new Fields[pipeLen];
			Fields[] keyFields1 = new Fields[pipeLen];

			for(int i_p=0; i_p<pipeLen; i_p++){
				String[] keyFieldArr = StringUtils.split(keyFieldStr[i_p], ',');
				keyFields[i_p] = new Fields();
				keyFields1[i_p] = new Fields();
				for (int i=0; i<keyFieldArr.length; i++){
					keyFields[i_p] = keyFields[i_p].append(new Fields(keyFieldArr[i]));
					keyFields1[i_p] = keyFields1[i_p].append(new Fields(keyFieldArr[i]+"_1"));
				}		
				//keyFields[i_p] = keyFields[i_p].append(new Fields("num_days", "date_"));
			}
			
			
			Pipe[] pAssembly = new HourlyAppendingPriceDataSubAssembly(pAssemblyBase_tmp, 
					keyFields, pipeNames).getTails();
			Pipe[] bAssembly = new HourlyAppendingBidDataSubAssembly(bAssemblyBase_tmp, 
					keyFields, pipeNames).getTails();

			Pipe[] mAssembly = new Pipe[pAssembly.length];
			Pipe[] dAssembly = new Pipe[pAssembly.length];
			for(int i=0; i<pAssembly.length; i++){
				Pipe pAssembly_tmp = new Pipe(pAssembly[i].getName()+"_"+i, pAssembly[i]);
				Pipe bAssembly_tmp = new Pipe(bAssembly[i].getName()+"_"+i, bAssembly[i]);
				Pipe[] mdAssembly = new ModelSubAssembly(pAssembly_tmp, bAssembly_tmp, pipeNames[i], keyFields[i], 
						keyFields1[i], config, numOfPoints, rSquareVal, maxPredWR, 1, pDateStr).getTails();
				mAssembly[i] = mdAssembly[0];
				dAssembly[i] = mdAssembly[1];
			}
			
			Fields hourKeyFields_p = new Fields();
			for(int i=0; i < hourKeyLen-1; i++)
				hourKeyFields_p = hourKeyFields_p.append(new Fields(keyFieldHourArr[i]));
			
			int countThValHour_tmp = Integer.parseInt(bidThValHour);
			Fields resultFields = hourKeyFields.append(bDataFields);
			Pipe bAssemblyBase_p = new Pipe(bAssemblyBase.getName() + "_p", bAssemblyBase);
 			if(hourKeyFields_p.size()>0){
 				Fields sortFields = new Fields("sum_bids");
 				sortFields.setComparator("sum_bids", Collections.reverseOrder());
				bAssemblyBase_p = new GroupBy(bAssemblyBase_p, hourKeyFields_p, sortFields);

				Buffer<?> accum = new OrderingCount(new Fields("bid_rank"));
				bAssemblyBase_p = new Every(bAssemblyBase_p, new Fields("sum_bids"), accum, Fields.ALL); 

				// Filtering out the data
				Filter<?> filterOutN = new FilterOutDataGEInt(countThValHour_tmp);
				bAssemblyBase_p = new Each(bAssemblyBase_p, new Fields("bid_rank"), filterOutN); 
				
			}
			
			bAssemblyBase_p = new Each(bAssemblyBase_p, resultFields, new Identity()); 

			Pipe bAssemblyBase_p_tmp = new Pipe(bAssemblyBase_p.getName()+"_tmp", bAssemblyBase_p);			
			
			pFields = pFields.append(new Fields("cum_wins"));
			Fields declaredFields = pFields.append(hourKeyFields1);
			declaredFields = declaredFields.append(bDataFields);

			//Joining pAssemblyBase and bAssemblyBase_p
			Pipe pAssemblyBase_p = new Pipe(pAssemblyBase.getName()+"_p", pAssemblyBase);			
			pAssemblyBase_p = new CoGroup(pAssemblyBase_p.getName(), pAssemblyBase_p, hourKeyFields, 
					bAssemblyBase_p_tmp, hourKeyFields, declaredFields, new InnerJoin());
			//Keeping the fields defined in pFields.
			Fields keptFields = pFields.append(bDataFields);
			pAssemblyBase_p = new Each(pAssemblyBase_p, keptFields, new Identity());	

			pAssemblyBase_p = new UIDataSubAssembly(pAssemblyBase_p, mAssemblyBase[0], hourKeyFields, hourKeyFields1,
					dataFields, modelDataFields, 1, pDateStr, config);
			
			Fields bDataFields1 = new Fields("sum_bids_1", "sum_nobids_1");
			Pipe[] bAssembly_p = new Pipe[bAssembly.length];
			for(int i=0; i<bAssembly.length; i++){
				Fields bFields1 = keyFields[i].append(bDataFields);
				Fields declaredFields1 = bFields1.append(hourKeyFields1);
				declaredFields1 = declaredFields1.append(bDataFields1);
				Pipe bAssemblyBase_p_unique = new Pipe(bAssemblyBase_tmp.getName()+"_unique", bAssemblyBase_p_tmp);
				bAssemblyBase_p_unique = new Unique(bAssemblyBase_p_unique, keyFields[i]);
				bAssembly_p[i] = new Pipe(bAssembly[i].getName()+"_p", bAssembly[i]);
				bAssembly_p[i] = new CoGroup(bAssembly_p[i].getName(), bAssembly_p[i], keyFields[i], 
						bAssemblyBase_p_unique, keyFields[i], declaredFields1, new InnerJoin());
				bAssembly_p[i] = new Each(bAssembly_p[i], bFields1, new Identity()); 
			}			
			
			Pipe[] pAssembly_p = new Pipe[pAssembly.length];
			for(int i=0; i<pAssembly.length; i++){
				Fields pFields1 = keyFields[i].append(pDataFields);
				pFields1 = pFields1.append(new Fields("cum_wins"));
				Fields declaredFields1 = pFields1.append(keyFields1[i]);
				declaredFields1 = declaredFields1.append(bDataFields);
				Pipe bAssembly_p_tmp = new Pipe(bAssembly_p[i].getName()+"_tmp", bAssembly_p[i]);
				pAssembly_p[i] = new Pipe(pAssembly[i].getName()+"_p", pAssembly[i]);
				pAssembly_p[i] = new CoGroup(pAssembly_p[i].getName(), pAssembly_p[i], keyFields[i], 
						bAssembly_p_tmp, keyFields[i], declaredFields1, new InnerJoin());
				//Keeping the fields defined in keptFields.
				keptFields = pFields1.append(bDataFields);
				pAssembly_p[i] = new Each(pAssembly_p[i], keptFields, new Identity());	

				pAssembly_p[i] = new UIDataSubAssembly(pAssembly_p[i], mAssembly[i], keyFields[i], keyFields1[i],
						dataFields, modelDataFields, 1, pDateStr, config);
			}

			int mult = 6;
			Pipe[] pvAssembly = new Pipe[(pipeLen+1)*mult];
            for(int i_p=0; i_p<pipeLen; i_p++){
            	pvAssembly[mult*i_p] = pAssembly[i_p];
            	pvAssembly[mult*i_p+1] = bAssembly[i_p];
            	pvAssembly[mult*i_p+2] = pAssembly_p[i_p];
            	pvAssembly[mult*i_p+3] = bAssembly_p[i_p];
            	pvAssembly[mult*i_p+4] = mAssembly[i_p];
            	pvAssembly[mult*i_p+5] = dAssembly[i_p];
            }
            
            pvAssembly[pipeLen*mult] = pAssemblyBase;
            pvAssembly[pipeLen*mult+1] = bAssemblyBase;
            pvAssembly[pipeLen*mult+2] = pAssemblyBase_p;
            pvAssembly[pipeLen*mult+3] = bAssemblyBase_p;
            pvAssembly[pipeLen*mult+4] = mAssemblyBase[0];
            pvAssembly[pipeLen*mult+5] = mAssemblyBase[1];
            
			setTails(pvAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
