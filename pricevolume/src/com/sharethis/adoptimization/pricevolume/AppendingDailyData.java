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

public class AppendingDailyData extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(AppendingDailyData.class);
	private static final long serialVersionUID = 1L;
	
	public AppendingDailyData(){}
	
	public AppendingDailyData(Map<String, Tap> sources, String outFilePath, String pDateStr, 
			int numOfDays, Configuration config, String pipeNameList, String keyFieldsList, 
			String pipeNameHour, String keyFieldsHour, String bidThValHour, String bidThValList) 
		throws IOException, ParseException, Exception
	{
		try{		
			// Defining the kept fields after price and bidder are merged.
			String pipeNameAll = pipeNameHour + ";" + pipeNameList;
			String keyFieldsAll = keyFieldsHour + ";" + keyFieldsList;
			String bidThValAll = bidThValHour + ";" + bidThValList;
			String[] bidThVals = StringUtils.split(bidThValAll,";");
			String[] pipeNames = StringUtils.split(pipeNameAll,";");
			int pipeLen = pipeNames.length;
			String[] keyFieldStr = StringUtils.split(keyFieldsAll, ";");
			Fields[] keyFields = new Fields[pipeLen];
            Fields[] keyFields1 = new Fields[pipeLen];
			Fields[] pFields = new Fields[pipeLen];
			Fields[] bFields = new Fields[pipeLen];
            Class[][] pTypes = new Class[pipeLen][];
            Class[][] bTypes = new Class[pipeLen][];

			Fields pDataFields = new Fields("winning_price","sum_wins","sum_mbids","cum_wins");
			Fields bDataFields = new Fields("sum_bids","sum_nobids");
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
				bFields[i_p] = keyFields[i_p].append(bDataFields);
				sLogger.info("i_p: " + i_p + "     pFields: " + pFields[i_p] + "    i_p: " 
						+ i_p + "     bFields: " + bFields[i_p]);
            	pTypes[i_p][keyFieldArr.length] = double.class;
                for(int i=1; i<pDataFields.size(); i++)
                	pTypes[i_p][keyFieldArr.length+i] = int.class;
                for(int i=0; i<bDataFields.size(); i++)
                	bTypes[i_p][keyFieldArr.length+i] = int.class;
			}

			Pipe[] pAssembly = new AppendingDailyPriceDataSubAssembly(sources, outFilePath, pDateStr, numOfDays, config, 
					keyFields, pFields, pipeNames, pTypes).getTails();

			Pipe[] bAssembly = new AppendingDailyBidDataSubAssembly(sources, outFilePath, pDateStr, numOfDays, config, 
					keyFields, bFields, pipeNames, bTypes).getTails();

			int numOfPoints = config.getInt("NumOfPoints", 15);
	        double rSquareVal = config.getFloat("RSquareVal", 0.90f);
	        double maxPredWR = config.getFloat("MaxPredWR", 0.75f);
			Fields modelDataFields = new Fields("model_type", "beta0", "beta1", 
					"r_square", "num_wp", "win_rate",  "min_wp", "max_wp", "max_wp_pred", "num_days", "date_");
			Fields dataFields = pDataFields.append(bDataFields);
			
			Pipe[] mAssembly = new Pipe[pAssembly.length];
			Pipe[] dAssembly = new Pipe[pAssembly.length];
			for(int i=0; i<pAssembly.length; i++){
				Pipe pAssembly_tmp = new Pipe(pAssembly[i].getName()+"_"+i, pAssembly[i]);
				Pipe bAssembly_tmp = new Pipe(bAssembly[i].getName()+"_"+i, bAssembly[i]);
				Pipe[] mdAssembly = new ModelSubAssembly(pAssembly_tmp, bAssembly_tmp, pipeNames[i], keyFields[i], 
						keyFields1[i], config, numOfPoints, rSquareVal, maxPredWR, numOfDays, pDateStr).getTails();
				mAssembly[i] = mdAssembly[0];
				dAssembly[i] = mdAssembly[1];
			}

			sLogger.info("The number of bid pipes: " + bAssembly.length);	
			Pipe[] bAssembly_p = new Pipe[bAssembly.length];

			Fields keyFields_p = new Fields();
			Fields keyFields_p1 = new Fields();
			for(int i=0; i < keyFields[0].size()-1; i++){
				keyFields_p = keyFields_p.append(new Fields(keyFields[0].get(i)));
				keyFields_p1 = keyFields_p1.append(new Fields(keyFields[0].get(i)+"_1"));
			}
			keyFields_p1 = keyFields_p1.append(new Fields(keyFields[0].get(keyFields[0].size()-1)+"_1"));
			
			int countThValHour_tmp = Integer.parseInt(bidThValHour);
			Fields resultFields = keyFields[0].append(bDataFields);

			bAssembly_p[0] = new Pipe(bAssembly[0].getName() + "_p", bAssembly[0]);

 			if(keyFields_p.size()>0){
 				Fields sortFields = new Fields("sum_bids");
 				sortFields.setComparator("sum_bids", Collections.reverseOrder());
				bAssembly_p[0] = new GroupBy(bAssembly_p[0], keyFields_p, sortFields);

				Buffer<?> accum = new OrderingCount(new Fields("bid_rank"));
				bAssembly_p[0] = new Every(bAssembly_p[0], new Fields("sum_bids"), accum, Fields.ALL); 

				// Filtering out the data
				Filter<?> filterOutN = new FilterOutDataGEInt(countThValHour_tmp);
				bAssembly_p[0] = new Each(bAssembly_p[0], new Fields("bid_rank"), filterOutN); 
			}
			bAssembly_p[0] = new Each(bAssembly_p[0], resultFields, new Identity()); 
			Pipe bAssembly_p_unique = new Pipe(bAssembly_p[0].getName()+"_unique", bAssembly_p[0]);

			Fields bDataFields1 = new Fields("sum_bids_1","sum_nobids_1");
			for(int i=1; i<bAssembly.length; i++){
				// Filtering out the data
				Fields bFields1 = keyFields[i].append(bDataFields);
				Fields declaredFields1 = bFields1.append(keyFields_p1);
				declaredFields1 = declaredFields1.append(bDataFields1);
				bAssembly_p[i] = new Pipe(bAssembly[i].getName()+"_p", bAssembly[i]);
				bAssembly_p_unique = new Unique(bAssembly_p_unique, keyFields[i]);
				sLogger.info("i = " + i + "   Price pipe name: " + bAssembly[i].getName()+"_p");
				bAssembly_p[i] = new CoGroup(bAssembly_p[i].getName(), bAssembly_p[i], keyFields[i], 
						bAssembly_p_unique, keyFields[i], declaredFields1, new InnerJoin());
				//Keeping the fields defined in pFields1.
				bAssembly_p[i] = new Each(bAssembly_p[i], bFields1, new Identity());	
			}			
			
			sLogger.info("The number of price pipes: " + pAssembly.length);	
			Pipe[] pAssembly_p = new Pipe[pAssembly.length];
			for(int i=0; i<pAssembly.length; i++){
				Fields pFields1 = keyFields[i].append(pDataFields);
				Fields declaredFields1 = pFields1.append(keyFields1[i]);
				declaredFields1 = declaredFields1.append(bDataFields);
				Pipe bAssembly_p_tmp = new Pipe(bAssembly_p[i].getName()+"_tmp", bAssembly_p[i]);
				sLogger.info("i = " + i + "   Price pipe name: " + pAssembly[i].getName()+"_p");
				pAssembly_p[i] = new CoGroup(pAssembly[i].getName()+"_p", pAssembly[i], keyFields[i], 
						bAssembly_p_tmp, keyFields[i], declaredFields1, new InnerJoin());

				//Keeping the fields defined in keptFields
				Fields keptFields = pFields1.append(bDataFields);			
				pAssembly_p[i] = new Each(pAssembly_p[i], keptFields, new Identity());	

				pAssembly_p[i] = new UIDataSubAssembly(pAssembly_p[i], mAssembly[i], keyFields[i], keyFields1[i],
						dataFields, modelDataFields, numOfDays, pDateStr, config);
			}
			
			sLogger.info("The number of pipes: " + pipeLen);	
			
			int mult = 6;
			Pipe[] pvAssembly = new Pipe[pipeLen*mult];
            for(int i_p=0; i_p<pipeLen; i_p++){
            	pvAssembly[mult*i_p] = pAssembly[i_p];
            	pvAssembly[mult*i_p+1] = bAssembly[i_p];
            	pvAssembly[mult*i_p+2] = pAssembly_p[i_p];
            	pvAssembly[mult*i_p+3] = bAssembly_p[i_p];
            	pvAssembly[mult*i_p+4] = mAssembly[i_p];
            	pvAssembly[mult*i_p+5] = dAssembly[i_p];
            }
			setTails(pvAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
