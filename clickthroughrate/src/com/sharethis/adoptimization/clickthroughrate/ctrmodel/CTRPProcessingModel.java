package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import cascading.operation.Aggregator;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * This is the assembly to read price confirmation data.
 */

public class CTRPProcessingModel extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(CTRPProcessingModel.class);
	private static final long serialVersionUID = 1L;
	
	public CTRPProcessingModel(Configuration config, Pipe dataAssembly, String xKeyFieldsList, 
			String modelName, String yModelField, String xModelField, String modelTypeList,
			String infile_postfix, String outfile_postfix) throws Exception{
		try{			
			Pipe[] yxAssembly = new Pipe[2];
			yxAssembly[0] = new Pipe("CTRP_"+modelName+infile_postfix, dataAssembly);
			
	    	String[] xKeyFieldsStr = StringUtils.split(xKeyFieldsList, ";");
			String[] xModelFieldsStr = StringUtils.split(xModelField, ";");
			String[] yModelFieldsStr = StringUtils.split(yModelField, ";");
			Fields modFields = new Fields();
			for(int i_y=0; i_y<yModelFieldsStr.length; i_y++){
				modFields = modFields.append(new Fields(yModelFieldsStr[i_y]));
			}

			Fields yDataField = new Fields("ctr");
			Fields xDataFields = new Fields();
			Fields xKeyFields = new Fields();
			Fields coeffFields = new Fields("beta0");
			for(int i=0; i<xModelFieldsStr.length; i++){
				String ctrData_i = null;
				for(int j=0; j<xKeyFieldsStr.length; j++){
					if(xModelFieldsStr[i].equalsIgnoreCase(xKeyFieldsStr[j])){
						ctrData_i = "ctr_"+j;
					}
				}
				if(ctrData_i==null){
					String msg = "x-fields: " + xModelFieldsStr[i] + " defined in the model was not found in the model data";
					throw new Exception(msg);
				}
				sLogger.info("Key Field Name: " + (xModelFieldsStr[i]));
				sLogger.info("Data Field Name: " + ctrData_i);
				String[] xModelFields = StringUtils.split(xModelFieldsStr[i], ",");
				for(int j=0; j<xModelFields.length; j++){
					if(!xKeyFields.contains(new Fields(xModelFields[j])))
						xKeyFields = xKeyFields.append(new Fields(xModelFields[j]));
				}
				xDataFields = xDataFields.append(new Fields(ctrData_i));
				coeffFields = coeffFields.append(new Fields("beta_"+i));
			}
			//Keeping useful fields.
			Fields inputFields = yDataField.append(xDataFields);
		    yxAssembly[0] = new Unique(yxAssembly[0], inputFields, 100000);
			Fields keptFields = modFields.append(inputFields);
			keptFields = keptFields.append(xKeyFields);
			keptFields = keptFields.append(new Fields("sum_imps"));
		    yxAssembly[0] = new Each(yxAssembly[0], keptFields, new Identity());			

		    if(yxAssembly[0]!=null){
		    	int numOfPoints = config.getInt("CTRPNumOfPoints", 5);
				int ptMult = config.getInt("CTRPMultThreshold", 3);
				double rSquareVal = config.getFloat("CTRPRSquareVal", 0.001f);
				double dataCons = config.getFloat("CTRPDataCons", 0.0005f);
				double dataMult = config.getFloat("CTRPDataMult", 1);
		    
				String[] modelTypes = StringUtils.split(modelTypeList,",");
		    
		    		Pipe[] mAssembly = new Pipe[modelTypes.length];
		    		for(int i=0; i<modelTypes.length; i++){
		    			int modelType = Integer.parseInt(modelTypes[i]);

		    			//Starting generating the models
		    			mAssembly[i] = new Pipe("CTRP_"+modelName+"_"+modelTypes[i], yxAssembly[0]);
		    			//Generating the models by looping the modelNames
		    			Fields modelOutputFields = coeffFields.append(new Fields("r_square", "adj_r_square", 
		    					"numPoints", "modelType"));
		    			Aggregator<?> CTRPModel = new CTRPModelRegression(modelOutputFields, 
		    					numOfPoints, ptMult, rSquareVal, modelType, dataCons, dataMult);
		    			mAssembly[i] = new GroupBy(mAssembly[i], modFields);
		    			mAssembly[i] = new Every(mAssembly[i], 
		    					yDataField.append(xDataFields), CTRPModel, Fields.ALL); 
		    		}		
		    		yxAssembly[1] = new Pipe("CTRP_"+modelName+outfile_postfix, new Merge(mAssembly));
		    		setTails(yxAssembly);
		    }else{
		    		setTails((Pipe) null);
		    }
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
	
	public CTRPProcessingModel(Configuration config, Pipe dataAssembly, String xKeyFieldsList, 
			String modelName, String yModelField, String xModelField, String xModelFieldKept, 
			String modelTypeList, String infile_postfix, String outfile_postfix) throws Exception{
		try{			
			Pipe[] yxAssembly = new Pipe[2];
			yxAssembly[0] = new Pipe("CTRP_"+modelName+infile_postfix, dataAssembly);
			
	    	String[] xKeyFieldsStr = StringUtils.split(xKeyFieldsList, ";");
			String[] xModelFieldsStr = StringUtils.split(xModelField, ";");
			String[] yModelFieldsStr = StringUtils.split(yModelField, ";");
			String[] xModelFieldsStrKept = StringUtils.split(xModelFieldKept, ";");
			Fields modFields = new Fields();
			for(int i_y=0; i_y<yModelFieldsStr.length; i_y++){
				modFields = modFields.append(new Fields(yModelFieldsStr[i_y]));
			}

			Fields yDataField = new Fields("ctr");
			Fields xDataFields = new Fields();
			Fields xKeyFields = new Fields();
			Fields coeffFields = new Fields("beta0");
			for(int i=0; i<xModelFieldsStr.length; i++){
				//sLogger.info("i = " + i + "   xModelFieldsStr: " + xModelFieldsStr[i]);
				String ctrData_i = null;
				for(int j=0; j<xKeyFieldsStr.length; j++){
					//sLogger.info("j = " + j + "   xKeyFieldsStr: " + xKeyFieldsStr[j]);
					if(xModelFieldsStr[i].equalsIgnoreCase(xKeyFieldsStr[j])){
						ctrData_i = "ctr_"+j;
						//sLogger.info("i = " + i + "   xModelFieldsStr: " + xModelFieldsStr[i] + "   j = " + j + "   xKeyFieldsStr: " + xKeyFieldsStr[j]);
					}
				}
				if(ctrData_i==null){
					String msg = "x-fields: " + xModelFieldsStr[i] + " defined in the model was not found in the model data";
					throw new Exception(msg);
				}
				//sLogger.info("Key Field Name: " + (xModelFieldsStr[i]));
				//sLogger.info("Data Field Name: " + ctrData_i);
				String[] xModelFieldsKept = StringUtils.split(xModelFieldsStrKept[i], ",");
				for(int j=0; j<xModelFieldsKept.length; j++){
					if(!xKeyFields.contains(new Fields(xModelFieldsKept[j])))
						xKeyFields = xKeyFields.append(new Fields(xModelFieldsKept[j]));
				}
				xDataFields = xDataFields.append(new Fields(ctrData_i));
				coeffFields = coeffFields.append(new Fields("beta_"+i));
			}
			//Keeping useful fields.
			Fields inputFields = yDataField.append(xDataFields);
		    yxAssembly[0] = new Unique(yxAssembly[0], inputFields, 100000);
			Fields keptFields = modFields.append(inputFields);
			keptFields = keptFields.append(xKeyFields);
			keptFields = keptFields.append(new Fields("sum_imps"));
		    yxAssembly[0] = new Each(yxAssembly[0], keptFields, new Identity());			

		    if(yxAssembly[0]!=null){
		    	int numOfPoints = config.getInt("CTRPNumOfPoints", 5);
				int ptMult = config.getInt("CTRPMultThreshold", 3);
				double rSquareVal = config.getFloat("CTRPRSquareVal", 0.000001f);
				double dataCons = config.getFloat("CTRPDataCons", 0.0005f);
				double dataMult = config.getFloat("CTRPDataMult", 1);
		    
				String[] modelTypes = StringUtils.split(modelTypeList,",");
		    
		    		Pipe[] mAssembly = new Pipe[modelTypes.length];
		    		for(int i=0; i<modelTypes.length; i++){
		    			int modelType = Integer.parseInt(modelTypes[i]);

		    			//Starting generating the models
		    			mAssembly[i] = new Pipe("CTRP_"+modelName+"_"+modelTypes[i], yxAssembly[0]);
		    			//Generating the models by looping the modelNames
		    			Fields modelOutputFields = coeffFields.append(new Fields("r_square", "adj_r_square", 
		    					"numPoints", "modelType"));
		    			Aggregator<?> CTRPModel = new CTRPModelRegression(modelOutputFields, 
		    					numOfPoints, ptMult, rSquareVal, modelType, dataCons, dataMult);
		    			mAssembly[i] = new GroupBy(mAssembly[i], modFields);
		    			mAssembly[i] = new Every(mAssembly[i], 
		    					yDataField.append(xDataFields), CTRPModel, Fields.ALL); 
		    		}		
		    		yxAssembly[1] = new Pipe("CTRP_"+modelName+outfile_postfix, new Merge(mAssembly));
		    		setTails(yxAssembly);
		    }else{
		    		setTails((Pipe) null);
		    }
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
}
