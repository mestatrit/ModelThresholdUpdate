package com.sharethis.adquality.IASReport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.sharethis.adquality.IASDownloadTool.IASConstants;
import com.sharethis.adquality.common.ConfigHelper;
import com.sharethis.common.constant.Constants;
import com.sharethis.common.error.ErrorCode;
import com.sharethis.common.helper.RuntimeHelper;
import com.sharethis.common.helper.app.AppConfig;

public class AdGroupFileWriterTest {
	@Test
	public void testWrite() throws Exception {
		String args[] = "-iap bin/res/ias_downloader.properties -iapp_path /mnt/adquality -il4jp bin/res/log4j.properties -mpb /mnt -ipb /mnt".split(" ");
		if( args.length==0 ) {
			System.out.println("Usage: com.sharethis.de.test.Tester -iap bin/res/app.properties -il4jp bin/res/log4j.properties");
			return;
		}
		
		Hashtable<String, String> params = com.sharethis.common.parser.CommandLineParser.parseCommandParams(args);		
		String il4jp = params.get(Constants.INPUT_LOG4J_PROPS);
		RuntimeHelper.setLog4J(il4jp, Constants.INPUT_LOG4J_PROPS_DEF);

		AppConfig config = new AppConfig();
		config.init(params);
		ConfigHelper.replaceAppTokens(config, params);
		Logger log = Logger.getLogger(AdGroupFileWriterTest.class);	
		
		log.setLevel(Level.INFO);			
				
		log.info("------------------------------------------------------------------------------------");
		log.info("Application properties");	
		List<String> keyList = new ArrayList<String>();
		Properties props = config.getProperties();
		Enumeration<Object> keys = props.keys();
		while (keys.hasMoreElements()) {
			String key = keys.nextElement().toString();
			keyList.add(key);
		}
		Collections.sort(keyList);
		for (int i = 0; i < keyList.size(); i++) {
			String key = keyList.get(i);
			String val = props.getProperty(key);
			log.info(key + ":" + val);
		}

		log.info("------------------------------------------------------------------------------------");
		String adgroup_list_file = config.get(IASConstants.ADGROUP_LIST_FILE);
		
		AdGroupFileWriter adgFW = new AdGroupFileWriter();
		if( adgFW.write(adgroup_list_file, config)==ErrorCode.ERROR_SUCCESS){
			log.error("Success, written file "+adgroup_list_file);
		}else{
			log.error("Error in = writing file "+adgroup_list_file);
		}
		
	}

}
