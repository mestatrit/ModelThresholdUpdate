package com.sharethis.adoptimization.clickthroughrate.ctrmodel;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;

public class CTRPGeneratingModel extends SubAssembly
{ 
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger sLogger = Logger.getLogger(CTRPGeneratingModel.class);
	
	public CTRPGeneratingModel(Configuration config, Pipe dataAssembly, String dataXField, 
			String modelNamesList, String yModelFieldsList, String xModelFieldsList, 
			String xModelFieldsListKept) throws Exception{
		try{
			String modelTypeList = config.get("CTRPModelTypeList");
			String infile_postfix = config.get("CTRPDataPostfix");
			String outfile_postfix = config.get("CTRPModelPostfix");
			sLogger.info("\nThe parameters: " + 
					"\nCTRModelNamesList = " + modelNamesList +
					"\nCTRYModelFieldsList = " + yModelFieldsList +
					"\nCTRXModelFieldsList = " + xModelFieldsList +
					"\nCTRXModelFieldsListKept = " + xModelFieldsListKept );
			if(modelNamesList==null||"null".equalsIgnoreCase(modelNamesList)
					||modelNamesList.isEmpty()){
				sLogger.info("No CTR predictive model is defined!");
			}else{			
				String[] modelNames = StringUtils.split(modelNamesList, "|");
				String[] yModelFields = StringUtils.split(yModelFieldsList, "|");
				String[] xModelFields = StringUtils.split(xModelFieldsList, "|");
				String[] xModelFieldsKept = StringUtils.split(xModelFieldsListKept, "|");
				if((modelNames.length==yModelFields.length)&&(modelNames.length>0)){
					Pipe[] modelAssembly = new Pipe[2*modelNames.length];
					for(int i=0; i<modelNames.length; i++){						   
						Pipe[] mAssembly = new CTRPProcessingModel(config, dataAssembly, dataXField, 
								modelNames[i], yModelFields[i], xModelFields[i], xModelFieldsKept[i], 
								modelTypeList, infile_postfix, outfile_postfix).getTails();	
						modelAssembly[i] = mAssembly[0];						    
						modelAssembly[modelNames.length+i] = mAssembly[1];
					}
					setTails(modelAssembly);
				}else{
					String msg = "The number of CTR predictive model names and the number of CTR predictive model key field lists are not the same!";
					throw new Exception(msg);							
				}
			}
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 	
}
