package com.sharethis.adoptimization.clickthroughrate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * This is the class to read hdfs data.
 */

public class ModReadingCountData
{

	private static final Logger sLogger = Logger.getLogger(ModReadingCountData.class);
	
	public int[] readingCountData(Configuration config, String inFilePath) 
		throws IOException, ParseException, Exception
	{
        int[] cntClick = new int[2];
		cntClick[0]=0;
		cntClick[1]=0;

		try{		
			String inFileName = inFilePath + config.get("ModDataCount")+"_hourly";
        	sLogger.info("Input File Name: " + inFileName);
			FileSystem fs = FileSystem.get(config);

			Path inFile = new Path(inFileName);
			if(fs.exists(inFile)){			
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

				for(String fileName:fileList){
					//sLogger.info("File Name: " + fileName);
					in = fs.open(new Path(fileName));
					GZIPInputStream stream = new GZIPInputStream(in);
					reader = new BufferedReader(new InputStreamReader(stream));		
					String line = null;
					while ((line = reader.readLine()) != null){
						String[] dataArr = StringUtils.split(line, "\t");
						int click_flag = (int) Double.parseDouble(dataArr[0]);
						cntClick[click_flag] = (int) Double.parseDouble(dataArr[1]);
					}
				}
				reader.close();
			}
			return cntClick;
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
