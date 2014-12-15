package com.sharethis.doreport.server;

import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.thread.QueuedThreadPool;

import com.sharethis.common.constant.Constants;
import com.sharethis.common.constant.RTBConstants;
import com.sharethis.common.constant.TokenConstants;
import com.sharethis.common.helper.RuntimeHelper;
import com.sharethis.common.helper.AppConfig;

public class ReportServer extends com.sharethis.common.jetty.server.ServerBase {
			
	public static final String NAME = "ReportServer";
	public static final String LOGGER_NAME = "WS";

	QueuedThreadPool threadPool;
	
	boolean checkServerConfig(AppConfig config) throws IOException {
		String[] paths = {
				config.get("html_file_path"), config.get("image_file_base_path")
		};
		
		log.error("ReportServer.checkServerConfig...");
		
		for(int i = 0; i < paths.length; i++) {
			this.log.error(paths[i]);
			File dir = new File(paths[i]);
			if(!dir.exists()) {
				String message = "File does not exist:" + paths[i] + ". Abort...";
				this.log.error(message);
				throw new IOException(message);
			}
		}
		return true;
	}
	
	public ReportServer(AppConfig config) throws IOException {
		super(config);
		
		this.log = Logger.getLogger(ReportServer.LOGGER_NAME);

		//Do not check the config for this version
		//checkServerConfig(config);
		
		int threadPooSize = config.getInt(RTBConstants.QUEUED_THREADPOLL_SIZE);
		this.threadPool = new QueuedThreadPool(threadPooSize);
		this.threadPool.setLowThreads(threadPooSize);
		this.threadPool.setMinThreads(config.getInt(RTBConstants.QUEUED_THREADPOLL_SIZE_MIN));		
		this.threadPool.setMaxIdleTimeMs(config.getInt(RTBConstants.THREADPOOL_IDLE_TIME_MAX));
		this.server.setThreadPool(this.threadPool);
		
		Context root = new Context(this.server, "/", Context.SESSIONS);
		//Html pages
		root.addServlet(HtmlServlet.class, "/*");
		//Reports
		root.addServlet(ReportServlet.class, "/report");
		//Images
		root.addServlet(ImageServlet.class, "/images/*");
		root.setAttribute(ReportServer.NAME, this);	
	}
		
	/**
	 * Create a required listener for the Jetty instance listening on the port
	 * provided. This wrapper and all subclasses must create at least one
	 * listener.
	 */
	protected Connector createBaseListener(AppConfig config) throws IOException {
		SelectChannelConnector ret = new SelectChannelConnector();
		ret.setLowResourceMaxIdleTime(config.getInt(RTBConstants.MAX_IDLE_TIME));
		ret.setAcceptQueueSize(config.getInt(RTBConstants.ACCEPT_QUEUE_SIZE));
		String host = config.get(RTBConstants.RTB_SERVER_ADDRESS);
		if(host == null || host.length() < 5) {
			host = java.net.InetAddress.getLocalHost().getHostAddress();
			Logger.getLogger(RTBConstants.LOGGER_NAME).info("Local Host:" + host); 
		}		
		ret.setHost(host);
		ret.setPort(config.getInt(RTBConstants.RTB_SERVER_PORT));		
		ret.setResolveNames(false);
		ret.setUseDirectBuffers(false);		
		return ret;
	}
			
	public static void main( String args[] ) throws Exception {
				
		if( args.length==0 ) {
			System.out.println("Usage: Server -iap bin/res/dors.properties -il4jp bin/res/log4j.properties -dop bin/res/deliveryOptimizer.properties -ipb /mnt -opb /mnt");
			return;
		}
		
		Hashtable<String, String> params = com.sharethis.common.parser.CommandLineParser.parseCommandParams(args);		
		String il4jp = params.get(Constants.INPUT_LOG4J_PROPS);
		RuntimeHelper.setLog4J(il4jp, Constants.INPUT_LOG4J_PROPS_DEF);

		Logger log = Logger.getLogger(ReportServer.LOGGER_NAME);	
		
		String ipb = params.get(Constants.INPUT_PATH_BASE);
		log.info(Constants.INPUT_PATH_BASE + ":" + ipb);							
				
		AppConfig config = AppConfig.getInstance();
		config.init(params);
		
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
			val = val.replace(TokenConstants.INPUT_PATH_BASE, ipb);
			props.setProperty(key, val);
			log.info(key + ":" + val);
		}
		
		com.sharethis.common.helper.image.ImageManager.getInstance().init(config);					

		//Add all commandline input params 
		for(String key : params.keySet()) {
			props.put("-" + key, params.get(key));
		}
		
		log.info("------------------------------------------------------------------------------------");
		
		ReportServer server = new ReportServer(config);

		server.start();				
		server.join();
	}	
}
