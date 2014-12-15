package com.sharethis.adoptimization.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


public class ConfigurationUtil
{
	private static Logger sLogger = Logger.getLogger(ConfigurationUtil.class);
		
    public static Configuration getConfig(String resFile) throws IOException {
		sLogger.info("The property file path is " + resFile + ".");
    	Configuration conf = new Configuration();
		InputStream in = new FileInputStream(new File(resFile));
//		InputStream in = ConfigurationUtil.class.getResourceAsStream(resFile);	
		Properties props = new Properties();
		if(in!=null){
			props.load(in);
			in.close();
			Enumeration enu = props.propertyNames();
			while(enu.hasMoreElements()){
				String keyName = (String) enu.nextElement();
				String valName = props.getProperty(keyName);
				sLogger.info("key: " + keyName + "   valName: " + valName);
				conf.set(keyName.trim(), valName.trim());
			}
		}
		sLogger.info("Loading the property file is done.");
		return conf;
    }
    
	public static Configuration setConf(String[] args) throws IOException{
		Configuration conf = new Configuration();
		String resFile = null;
		sLogger.info("The first parameter: " + args[0]);
		if(args!=null && args[0]!=null && !args[0].contains("=") && args[0].contains("properties")){
			resFile = args[0];
		}
		conf = ConfigurationUtil.getConfig(resFile);
		if(!args.equals(null)){
			int argsLen = args.length;
			String[] parmStr = new String[2];
			for(int i=0; i<argsLen; i++){
//				sLogger.info("i = "+i+"   args = "+args[i]);
				if(args[i].contains("=")){
					parmStr = StringUtils.split(args[i],'=');
					sLogger.info("i = "+i+"   "+parmStr[0] + " = " + parmStr[1]);
					conf.set((parmStr[0]).trim(), (parmStr[1]).trim());
				}
			}
		}
		conf.set("mapred.child.java.opts","-Xmx3000m");
		return conf;
	}
	
	public static Configuration setParms(String[] args) throws IOException{
		Configuration conf = new Configuration();
		if(!args.equals(null)){
			int argsLen = args.length;
			String[] parmStr = new String[2];
			for(int i=0; i<argsLen; i++){
//				sLogger.info("i = "+i+"   args = "+args[i]);
				if(args[i].contains("=")){
					parmStr = StringUtils.split(args[i],'=');
					sLogger.info("i = "+i+"   "+parmStr[0] + " = " + parmStr[1]);
					conf.set((parmStr[0]).trim(), (parmStr[1]).trim());
				}
			}
		}
		conf.set("mapred.child.java.opts","-Xmx3000m");
		return conf;
	}
}
