package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;

/**
 * This is the assembly to read price confirmation data.
 */

public class CTRPComputingPrediction
{
	private static Logger sLogger = Logger.getLogger(CTRPComputingPrediction.class.getName());
	private double[] modelCoeffs;
	private double intercept = 0;
	private String modelStr =null;
	private String[] keyNames = null;
	
	public String[] getKeyNames() {
		return keyNames;
	}

	public void setKeyNames(String[] keyNames) {
		this.keyNames = keyNames;
	}

	public CTRPComputingPrediction(){
	}

	public CTRPComputingPrediction(Configuration config) throws Exception{
		sLogger.info("Loading the CTR predictive model from db table starts ...");
		CTRPQueryingModel qModel = new CTRPQueryingModel();
		modelStr = qModel.loadingModelMap(config);
		intercept = qModel.getIntercept();
		modelCoeffs = qModel.getModelCoeffs();
		keyNames = qModel.getKeyNames();
		sLogger.info("intercept: " + intercept);
		for(int i=0; i<modelCoeffs.length; i++){
			sLogger.info(keyNames[i] + ": " + modelCoeffs[i]);
		}
	}

	public double computingPrediction(double[] ctrBase){
		double ctrVal = intercept;
		for(int i=0; i<ctrBase.length; i++)
			ctrVal += ctrBase[i]*modelCoeffs[i];
		if(ctrVal<0)
			ctrVal=0;
		return ctrVal;
	}

	public void predictingCTR(Configuration config, String inFilePath, String outPath, String allOutPath) 
			throws IOException, ParseException, Exception
		{
			try{		
				
				sLogger.info("Loading the CTR base from db table starts ...");
				CTRPComputingPrediction CTRP = new CTRPComputingPrediction(config);
				String[] keyNames = CTRP.getKeyNames();
				long time_s0 = System.currentTimeMillis();
				CTRPQueryingCTRBase qCTR = new CTRPQueryingCTRBase();
				Map<String, Map<String,Double>> ctrBaseMap = qCTR.quaryingCTRBase(config, keyNames);
				long time_s1 = System.currentTimeMillis();

				BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outPath)));
				BufferedWriter bw_all = new BufferedWriter(new FileWriter(new File(allOutPath)));
				
	        	sLogger.info("Input File Name: " + inFilePath);
				FileSystem fs = FileSystem.get(config);

				Path inFile = new Path(inFilePath);
							
				List<String> fileList = new ArrayList<String>(0);
		        FileStatus[] fsstatus = fs.listStatus(inFile);
		        for(int i = 0; i < fsstatus.length; i++) {
		        	FileStatus fst = fsstatus[i];
		        	String name = fst.getPath().toString();
		        	//sLogger.info("Index: " + i + "     File Name: " + name);
		        	if(name.contains("part")){
			        	//sLogger.info("Index: " + i + "     File Name: " + name);
			        	if(!fileList.contains(name)) {
			        		fileList.add(name);
			        	}
		        	}
		        }

				FSDataInputStream in = null;
				BufferedReader reader = null;

				double sum_imps = 0;
				double sum_clicks = 0;
				double overall_ctr_pred = 0;
				
		        int cnt = 0;
				for(String fileName:fileList){
		        	//sLogger.info("File Name: " + fileName);
		        	in = fs.open(new Path(fileName));
		        	GZIPInputStream stream = new GZIPInputStream(in);
					reader = new BufferedReader(new InputStreamReader(stream));
			
					String line = null;
					while ((line = reader.readLine()) != null){
						String[] dataArr = StringUtils.split(line, "\t");
						String[] outArr = new String[3];
						outArr[0] = dataArr[7];
						outArr[1] = dataArr[27];
						outArr[2] = dataArr[2];
						double[] ctrBase = qCTR.quaryingCTRBase(ctrBaseMap, keyNames, outArr);				
						double ctrPred = computingPrediction(ctrBase);
						bw.write("\nCount: " + cnt + "\n");
						for(int i_k=0; i_k<keyNames.length; i_k++)
							bw.write(keyNames[i_k] + ":" + outArr[i_k] + ":" + ctrBase[i_k] + "\n");
						bw.write("Predicted CTR: " + ctrPred + "\n");

						double[] valArr = new double[2];
						valArr[0] = Double.parseDouble(dataArr[28]);
						valArr[1] = Double.parseDouble(dataArr[29]);
						
						sum_clicks += valArr[0];
						sum_imps += valArr[1];						
						overall_ctr_pred += valArr[1]*ctrPred;
						
						bw_all.write(line + "\t" + cnt + " \t" + ctrPred);
						cnt++;
					}
				}
				bw.write("Overall CTR:" + (sum_clicks/sum_imps*100) + "\n");
				bw.write("Overall Predicted CTR: " + (overall_ctr_pred/sum_imps*100) + "\n");
				reader.close();
				long time_s2 = System.currentTimeMillis();
				bw.write("Loading ctr base time: " + (time_s1-time_s0)/1000 + "\n");
				bw.write("Computing the predicted ctr time: " + (time_s2-time_s1)/1000 + "\n");
				bw.close();		
				bw_all.close();
				sLogger.info("Loading the CTR base from db table is done!");
			}catch(Exception ee){
				sLogger.info(ee.toString());
				throw new Exception(ee);
			}
		}
	
	public static void testTransData(String[] args) {
		try{
						
			Configuration config = ConfigurationUtil.setConf(args);
			  			
			CTRPComputingPrediction CTRP = new CTRPComputingPrediction(config);
			String allOutPath = "output/ctr_pred_all.txt";
			String outPath = "output/ctr_pred_file.txt";
			sLogger.info("CTR pred file path:" + outPath);					
			String inPath = "input/ic_pb_hourly";			
			CTRP.predictingCTR(config, inPath, outPath, allOutPath);				
		}
		catch(Exception e) {
			sLogger.error(e);
		}		
	}

	public static void main(String[] args) {
		CTRPComputingPrediction.testTransData(args);
		//CTRPComputingPrediction.testAggData(args);
	}

	public static void testAggData(String[] args) {
		try{
						
			Configuration config = ConfigurationUtil.setConf(args);

			CTRPComputingPrediction CTRP = new CTRPComputingPrediction(config);
			String[] keyNames = CTRP.getKeyNames();
			
			long time_s0 = System.currentTimeMillis();
			sLogger.info("Loading the CTR base from db table starts ...");
			CTRPQueryingCTRBase qCTR = new CTRPQueryingCTRBase();
			Map<String, Map<String,Double>> ctrBaseMap = qCTR.quaryingCTRBase(config, keyNames);

			long time_s1 = System.currentTimeMillis();

			String[] keyValues = null;
			String inPath = "input/ctr_key_value.txt";
			  
			FileInputStream fstream = new FileInputStream(inPath);			  
			// Get the object of DataInputStream			  
			DataInputStream in = new DataInputStream(fstream);			  
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));

			String outPath = "output/ctr_pred_file.txt";
			sLogger.info("CTR pred file path:" + outPath);		
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outPath)));
			String line = null;
			int numLine = 0;
			while ((line = reader.readLine()) != null){
				keyValues = StringUtils.split(line, "\t");				
				double[] ctrBase = qCTR.quaryingCTRBase(ctrBaseMap, keyNames, keyValues);				
				double ctrPred = CTRP.computingPrediction(ctrBase);
				bw.write("\nCount: " + numLine + "\n");
				for(int i=0; i<keyNames.length; i++)
					bw.write(keyNames[i] + ":" + keyValues[i] + ":" + ctrBase[i] + "\n");
				bw.write("Predicted CTR: " + ctrPred + "\n");
				numLine++;
			}		
			long time_s2 = System.currentTimeMillis();
			bw.write("Loading ctr base time: " + (time_s1-time_s0)/1000 + "\n");
			bw.write("Computing the predicted ctr time: " + (time_s2-time_s1)/1000 + "\n");
			reader.close();
			bw.close();			
			sLogger.info("Loading the CTR base from db table is done!");
		}
		catch(Exception e) {
			sLogger.error(e);
		}		
	}
}
