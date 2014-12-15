package com.sharethis.adoptimization.clickthroughrate.ctrmodel;


import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.filter.Not;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;

import org.apache.log4j.Logger;

import com.sharethis.adoptimization.clickthroughrate.AggregatingCTRAll;
import com.sharethis.adoptimization.clickthroughrate.ConstructingCTRKeys;
import com.sharethis.adoptimization.common.AssigningOneConstantId;
import com.sharethis.adoptimization.common.FilterOutDataLEDouble;
import com.sharethis.adoptimization.common.FilterOutNullData;
import com.sharethis.adoptimization.common.MassagingDefaultCTRValue;
import com.sharethis.adoptimization.common.MassagingKeyCTRValue;

/**
 * This is the assembly to read price confirmation data.
 */

public class CTRPMassagingCTRBaseData extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(CTRPMassagingCTRBaseData.class);
	private static final long serialVersionUID = 1L;
	
	public CTRPMassagingCTRBaseData(Fields inKeyFields_i, Fields inKeyFields, Pipe inAssembly, int modThresholdVal, 
			int baseThresholdVal, double ctrThresholdVal, String all_id, int i, String masKeyVal) throws Exception{
		try{		    
    		Pipe outAssembly = new Pipe(inAssembly.getName()+"_base", inAssembly); 

    		Fields newField = new Fields(all_id);
    		Fields resFields = new Fields("sum_imps", "sum_clicks", "sum_cost", 
    				"ctr", "e_cpm", "e_cpc");
	    	int newIdVal = 1;
		    Function<?> aFunc = new AssigningOneConstantId(newField, newIdVal);
		    inAssembly = new Each(inAssembly, aFunc, Fields.ALL); 

		    //Pipe inAssembly_invalid = new Pipe(inAssembly.getName()+"_invalid", inAssembly); 
		    //Separating the rows with sum_imps<=1000;
		    //Filter<?> filter = new FilterOutDataLEDouble(thresholdVal);
		    //inAssembly_invalid = new Each(inAssembly_invalid, new Fields("sum_imps"), new Not(filter)); 
	    	//String onePipeName = inAssembly_invalid.getName() + "_overall_agg";
	    	//Pipe oneAssembly = new Pipe(onePipeName, inAssembly_invalid);
		    
		    Fields baseFields = new Fields("sum_imps", "sum_clicks", "ctr");
	    	String onePipeName = inAssembly.getName() + "_overall_agg";
	    	Pipe oneAssembly = new Pipe(onePipeName, inAssembly);
	    	oneAssembly = new AggregatingCTRAll(new Pipe[]{oneAssembly}, onePipeName, newField).getTails()[0];
	    	Fields keptFields = newField.append(baseFields);
	    	oneAssembly = new Each(oneAssembly, keptFields, new Identity());
	    	
	    	Fields allFields = inKeyFields_i;
			allFields = allFields.append(resFields);
			allFields = allFields.append(newField);
			allFields = allFields.append(new Fields("key_id_all", "sum_imps_all", "sum_clicks_all", "ctr_all"));
	    	inAssembly = new CoGroup(inAssembly.getName(), inAssembly, newField, oneAssembly, newField, allFields, new LeftJoin());
	    	
	    	int numKeyFields = inKeyFields.size();    		
    		Fields defaultFields = inKeyFields.append(baseFields);
		    Filter<?> dFilter = new FilterOutDataLEDouble(baseThresholdVal);
		    outAssembly = new Each(outAssembly, new Fields("sum_imps"), dFilter); 
	    	outAssembly = new Each(outAssembly, defaultFields, new Identity());

       		Function<?> dFunc = new MassagingDefaultCTRValue(defaultFields, masKeyVal, numKeyFields, oneAssembly.getName());
       		Pipe defaultAssembly = new Each(oneAssembly, keptFields, dFunc); 
       		//Appending the default record into the base ctr pipe.
       		outAssembly = new Pipe(outAssembly.getName(), new Merge(outAssembly, defaultAssembly));
 
       		if(outAssembly.getName().equalsIgnoreCase("crtv_adg_base")||outAssembly.getName().equalsIgnoreCase("usg_base")
           		||outAssembly.getName().equalsIgnoreCase("set_base")||outAssembly.getName().equalsIgnoreCase("ctl1_base")){
           		//Filtering out the records with id='null' or id is empty.
           		Filter<?> filterOutNull = new FilterOutNullData();
           		outAssembly = new Each(outAssembly, inKeyFields, filterOutNull); 
           	}
       		
    		//Constructing the data file for rtb to pick up the ctr base data.
    		Pipe rtbAssembly = new Pipe("rtb_" + outAssembly.getName(), outAssembly);
			Fields resultFields_i1 = new Fields("type", "keys");

			Fields allFields_i1 = resultFields_i1.append(baseFields);

			String ctrType = inAssembly.getName();
			///////////////////////////////////////////////////////////////////
			//The following lines are added to support the special type 'crtv' for rtb
			if((inAssembly.getName()).equalsIgnoreCase("crtv_adg"))
				ctrType = "crtv";
			if((inAssembly.getName()).equalsIgnoreCase("crtv"))
				ctrType = "crtv_0";
//			if((inAssembly.getName()).equalsIgnoreCase("set"))
//				ctrType = "set_all";
//			if((inAssembly.getName()).equalsIgnoreCase("mdst_set")){
//				Filter<?> filterOutNull = new FilterOutDataNEStr("null");
//				rtbAssembly = new Each(rtbAssembly, new Fields("deviceType"), filterOutNull); 
//				inKeyFields = new Fields("setting_id");
//				Fields kFields = inKeyFields.append(baseFields);
//		    	rtbAssembly = new Each(rtbAssembly, kFields, new Identity());
//				ctrType = "set";
//			}
			////////////////////////////////////////////////////////////////////

			if(inAssembly.getName().equalsIgnoreCase("set")||inAssembly.getName().equalsIgnoreCase("net")){
			    Filter<?> ctrFilter = new FilterOutDataLEDouble(ctrThresholdVal);
			    rtbAssembly = new Each(rtbAssembly, new Fields("ctr"), new Not(ctrFilter)); 
			}

			Function<?> keyFunc = new ConstructingCTRKeys(resultFields_i1, ctrType);
			rtbAssembly = new Each(rtbAssembly, inKeyFields, keyFunc, allFields_i1);
			
			//Massaging the data for ctr modeling
    		Fields masKeyFields = new Fields();
    		for(int i_num=0; i_num<numKeyFields; i_num++){
    			masKeyFields = masKeyFields.append(new Fields("keyId_"+i_num));
    		}
    		//String masKeyVal = "ST_DEFAULT";
    		Fields masDataFields = new Fields("sum_imps_"+i, "sum_clicks_"+i, "ctr_"+i);
    		Fields masFields = masKeyFields.append(masDataFields);
    		Fields masOutFields = allFields.append(masFields);
    		Function<?> mFunc = new MassagingKeyCTRValue(masFields, modThresholdVal, masKeyVal, numKeyFields);
    		inAssembly = new Each(inAssembly, allFields, mFunc, masOutFields); 
  
	    	keptFields = inKeyFields_i.append(new Fields("ctr_"+i));
	    	inAssembly = new Each(inAssembly, keptFields, new Identity());
  		
			setTails(inAssembly, outAssembly, rtbAssembly);			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
}
