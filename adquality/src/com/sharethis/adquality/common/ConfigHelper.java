package com.sharethis.adquality.common;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;

import com.sharethis.adquality.IASDownloadTool.EnvConstants;
import com.sharethis.common.helper.app.AppConfig;

public class ConfigHelper {
	public static void replaceAppTokens(AppConfig config, java.util.Map<String, String> params) {
		//Add variable references
		String value = params.get(EnvConstants.iapp_path);
		params.put(EnvConstants.APP_PATH, value);
		copyParam(config, value, EnvConstants.APP_PATH);
		//
		Set<String> kSet = params.keySet();
		Properties props = config.getProperties();		
		Enumeration<Object> keys = props.keys();
		while (keys.hasMoreElements()) {
			String key = keys.nextElement().toString();
			String val = props.getProperty(key);			
			for(String k : kSet) {
				val = val.replace(k, params.get(k));
			}
			props.setProperty(key, val);	
		}
	}
	public static void copyParam(AppConfig config, String value,
			String param) {
		if(value!=null){
			config.put(param, value);
		}
		
	}
}
