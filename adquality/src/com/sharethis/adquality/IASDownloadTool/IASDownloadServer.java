package com.sharethis.adquality.IASDownloadTool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.sharethis.adquality.bo.IasData;
import com.sharethis.adquality.common.ConfigHelper;
import com.sharethis.common.constant.Constants;
import com.sharethis.common.error.ErrorCode;
import com.sharethis.common.helper.RuntimeHelper;
import com.sharethis.common.helper.app.AppConfig;
import com.sharethis.common.helper.log.LogHelper;


public class IASDownloadServer {
	
	
	private int MaxOutstandingCalls = 0;
	static Logger log = Logger.getLogger(IASDownloadServer.class.getName());
	
	StatsMonitor statsMonitor = new StatsMonitor();
	ThrottleService throttleService = new ThrottleService();
	ExecutorService executor; //= Executors.newFixedThreadPool(nThreads)();
	Queue<HttpURLConnection> urlConnectionPool;
	
	private int urlCount;
	private String urlDir;
	private String urlFileRegEx;
	private String outputFile;
	private List<String> urlList = new LinkedList<String>();
	private int maxIasRetries=0;
	private int urlConnectionSize;
	private String iasHost;
	private int iasPort;
	private String iasCallFormat;
	private int iasTimeout;
	private boolean httpKeepAlive;
	private int httpMaxConnections;
	private Map<String, FutureContext> futureContextMap = new HashMap<String, FutureContext>();
	private int iasMaxRequestPerPeriod;
	private int iasPeriod;
	private boolean ignoreIasAction;
	private Set<String> siSet = new HashSet<String>();
	private long futureTimeout;
	
	public int init(AppConfig config, Map<String, String> params) throws IOException{
		int ret=-1;
		String il4jp = params.get(Constants.INPUT_LOG4J_PROPS);
		RuntimeHelper.setLog4J(il4jp, Constants.INPUT_LOG4J_PROPS_DEF);
		
		//log.setLevel(Level.ERROR);			
				
		log.error("------------------------------------------------------------------------------------");
		Properties props = config.getProperties();
		List<String> keyList = new ArrayList<String>();
		Enumeration<Object> keys = props.keys();
		while (keys.hasMoreElements()) {
			String key = keys.nextElement().toString();
			keyList.add(key);
		}
		Collections.sort(keyList);
		for (int i = 0; i < keyList.size(); i++) {
			String key = keyList.get(i);
			log.error(key + ": " + props.getProperty(key));
		}
		this.urlDir = config.get(IASConstants.URL_DIR);
		this.urlFileRegEx =  config.get(IASConstants.URL_FILE_REGEX);
		this.outputFile = config.get(IASConstants.OUTPUT_FILE);
		this.ignoreIasAction = config.getBool(IASConstants.IGNORE_IAS_ACTION);
		this.maxIasRetries = config.getInt(IASConstants.MAX_IAS_RETRIES);
		this.iasTimeout = config.getInt(IASConstants.IAS_TIMEOUT);
		//create url connection pool
		this.iasCallFormat=config.get(IASConstants.IAS_CALL_FORMAT);
		this.iasHost = config.get(IASConstants.IAS_HOST);
		this.iasPort = config.getInt(IASConstants.IAS_PORT);
		this.httpKeepAlive = config.getBool(IASConstants.HTTP_KEEP_ALIVE);
		System.setProperty(IASConstants.HTTP_KEEP_ALIVE, Boolean.toString(this.httpKeepAlive));
		this.httpMaxConnections = config.getInt(IASConstants.HTTP_MAX_CONNECTIONS);
		System.setProperty(IASConstants.HTTP_MAX_CONNECTIONS, Integer.toString(this.httpMaxConnections));
		this.urlConnectionSize=config.getInt(IASConstants.URL_CONNECTION_SIZE);
		//throttle config
		this.iasMaxRequestPerPeriod= config.getInt(IASConstants.IAS_MAX_REQ_PER_PERIOD);
		this.iasPeriod= config.getInt(IASConstants.IAS_PERIOD_MSEC);
		this.MaxOutstandingCalls = config.getInt(IASConstants.MAX_OUTSTANDING_CALLS);
		//for(int i=0; i<this.urlConnectionSize;i++){
		//	this.urlConnectionPool.add(new HttpURLConnection(this.iasHost,this.iasPort));
		//}
		//start thread pool
		executor= Executors.newFixedThreadPool(config.getInt(IASConstants.MAX_DL_THREADS));
		throttleService.init(this.iasMaxRequestPerPeriod, this.iasPeriod);
		throttleService.start();
		log.error("Throttle: period="+this.iasPeriod+", maxReqPerPeriod="+this.iasMaxRequestPerPeriod);
		//get keep alive
		log.error(IASConstants.HTTP_KEEP_ALIVE+":"+System.getProperty(IASConstants.HTTP_KEEP_ALIVE));
		log.error(IASConstants.HTTP_MAX_CONNECTIONS+":"+System.getProperty(IASConstants.HTTP_MAX_CONNECTIONS));
		
		return ret;
	}

	public static enum IAS_ACTION{
		PASSED("passed"),
		UNKNOWN("unknown");
		private String name;
		IAS_ACTION(String n){ this.name=n;}
		public static boolean isPassed(String s){ return s.compareToIgnoreCase(PASSED.name)==0;}
	}
	public class IASResponse {
		public IasData iasData;
		public String rawData="";
		String url;
		public int respCode=0;
		public boolean hasData() {
			return (iasData.uem.iviab!=null); 
		}
	}

	public class FutureContext {
		private String url;
		private Future<IASResponse> future;
		private int retries=0;
		
		public FutureContext(String url, Future<IASResponse> future2) {
			this.url=url;
			this.future=future2;
		}

		public Future<IASResponse> getFuture() { return this.future; }
		public String getUrl() { return url; }
		public int getRetries() { return retries; }
		public void setRetries(int retries) { this.retries = retries; }
	}
	class CBProcessUrl implements Callable<IASResponse> {
		String url=null;
		int retries=0;
		CBProcessUrl(String url){
			this.url=url;
		}
		@Override
		public IASResponse call() throws Exception {
			IASResponse iasResp = new IASResponse();
			iasResp.url=URLEncoder.encode(url,"UTF-8");
			String callUrl = IASDownloadServer.this.iasCallFormat+ URLEncoder.encode(url,"UTF-8");
			log.info("Request:"+callUrl);
			URL connUrl = new URL(callUrl);
			HttpURLConnection conn = (HttpURLConnection)connUrl.openConnection();
			
			conn.setConnectTimeout(IASDownloadServer.this.iasTimeout);
			int ret = readResponse(conn, iasResp);
			conn.disconnect();
			if(ret==ErrorCode.ERROR_SUCCESS){
				return iasResp;
			}
			
			return null;
		}
		int readResponse( HttpURLConnection conn, IASResponse iasResp){
			int ret = ErrorCode.ERROR_SUCCESS;
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			byte[] buf = new byte[4096];
	        try {
	        	InputStream is = conn.getInputStream();
	        	int res=0;
				while ((res = is.read(buf)) > 0) {
				    os.write(buf, 0, res);
				}
				is.close();
				String resp = new String(os.toByteArray());
				iasResp.rawData=resp;
				log.info("Response:"+resp);
				Gson gson = new Gson();
				IasData iasData = gson.fromJson(resp, IasData.class);
				iasResp.iasData = iasData;
			} catch (IOException e) {
				ret=ErrorCode.ERROR_INVALID_DATA;
				//e.printStackTrace();
				log.error("IOException:"+e.toString());
				try {
		            int respCode = conn.getResponseCode();
		            iasResp.respCode=respCode;
		            InputStream es = conn.getErrorStream();
		            ret = 0;
		            // read the response body
		            while ((ret = es.read(buf)) > 0) {
		                os.write(buf, 0, ret);
		            }
		            // close the errorstream
		            es.close();
		            log.error( "Error response " + respCode + ": " + 
		               new String(os.toByteArray()));
		        } catch(IOException ex) {
		            //throw ex;
		        	//ex.printStackTrace();
		        	log.error("IOException^2:"+e.toString());
		        }
				
			}catch(JsonSyntaxException e){
				ret=ErrorCode.ERROR_INVALID_DATA;
				log.error("JsonSyntaxException: raw data"+iasResp.rawData+"\n"+e.toString());
			}
	        return ret;
		}
		void setRetries(int v) { this.retries=v;}
		int getRetries(){ return this.retries; }
	}
	public int processUrls() throws IOException{
		int ret=ErrorCode.ERROR_SUCCESS;
		ret= read_urls();
		//put urls in queue
		
		if(ret==ErrorCode.ERROR_SUCCESS){
			List<Future<Boolean>> list = new ArrayList<Future<Boolean>>();
			Queue<String> urlQueue = new ConcurrentLinkedQueue<String>();
			urlQueue.addAll(this.urlList);
			this.statsMonitor.setTotal(urlList.size());
			//Open output file
			BufferedWriter bw=null;
			try {
				bw = new BufferedWriter(new FileWriter(this.outputFile));
			} catch (IOException e) {
				log.error("Cannot find output file "+ this.outputFile);
				e.printStackTrace();
				return ErrorCode.ERROR_FILE_NOT_FOUND;
			}
			//start progress thread
			statsMonitor.start();
			while(!urlQueue.isEmpty() || !futureContextMap.isEmpty()){
				//log.error("URLQueue="+urlQueue.size()+", futureMap="+futureContextMap.size());
				String url=null;
				if(!urlQueue.isEmpty()){
					url=urlQueue.remove();
				}
				if(url!=null){
					while(!this.throttleService.submitEvent(true)){
						log.info("Cannot submit");
					}
					this.statsMonitor.incSubmit();
					//log.error("Submitting...");

					Future<IASResponse> future= this.executor.submit(new CBProcessUrl(url) );
					FutureContext fctx= new FutureContext(url, future);
					futureContextMap.put(url,fctx);

				}
				if(urlQueue.isEmpty() || futureContextMap.size() >= this.MaxOutstandingCalls){
					processFutures(bw);
				}
				
			}
			bw.close();
			statsMonitor.stop();
			statsMonitor.writeStats();
			log.error("DONE: URLQueue="+urlQueue.size()+", futureMap="+futureContextMap.size());
		}
		return ret;
	}
	private void processFutures(BufferedWriter bw) throws IOException {
		List<String> completedKeys = new ArrayList<String>();
		Map<String, FutureContext> resubmitMap = new HashMap<String, FutureContext>();
		for(Map.Entry<String, FutureContext> entry: this.futureContextMap.entrySet()){
			String key=entry.getKey(); 
			FutureContext futureContext = entry.getValue();
			Future<IASResponse> future = futureContext.getFuture();
				//add to completed array
				//this.futureContextMap.remove(entry.getKey());
				//check status and write to output file
				IASResponse iasResp=null;
				boolean resubmit=false;
				try {
					iasResp = future.get(this.iasTimeout, TimeUnit.MILLISECONDS);
					if(iasResp==null){
						resubmit=true;
						log.error(key+" return null iaspResp!");
					}else {
						if(this.siSet.contains(iasResp.iasData.si)){
							log.error("URL repeated SI:"+futureContext.url+ "->"+iasResp.iasData.si);
							this.statsMonitor.incRepeatedSi();
						}else if (iasResp.hasData()){
							this.siSet.add(iasResp.iasData.si);
							//only write si
							bw.write(iasResp.iasData.si
									+IASConstants.URL_FILE_DELIMITER+iasResp.iasData.toJson(false)
									//+IASConstants.URL_FILE_DELIMITER+iasResp.rawData
									//+IASConstants.URL_FILE_DELIMITER+iasResp.url
									+"\n");
						}else{
							log.error("URL No VisData:"+futureContext.url+ "->"+iasResp.iasData.si);
							this.statsMonitor.incNoVisData();
						}

					}
				
				} catch (CancellationException e){
					resubmit=true;
					log.error(LogHelper.formatMessage(e));
				} catch (InterruptedException e) {
					resubmit=true;
					log.error(LogHelper.formatMessage(e));
				} catch (ExecutionException e) {
					resubmit=true;
					log.error(LogHelper.formatMessage(e));
				} catch (TimeoutException e) {
					log.error(LogHelper.formatMessage(e));
					resubmit=true;
				}
				//all keys are removed, and if not done are canceled! to avoid leakage
				completedKeys.add(key);
				if(!future.isDone()) future.cancel(true);
				
				if(resubmit){
					resubmitMap.put(key,futureContext);
					//reSubmitUrl(key, futureContext);
				}else{
					this.statsMonitor.incDoneCount();
				}
		}
		//remove completed keys from futureMap
		for(String key:completedKeys ){
			this.futureContextMap.remove(key);
		}
		for(Map.Entry<String, FutureContext> entry: resubmitMap.entrySet()){
			reSubmitUrl(entry.getKey(), entry.getValue());
		}
	}

	private void reSubmitUrl(String key, FutureContext prevFutureContext) {
		if(prevFutureContext.getRetries()>= this.maxIasRetries){
			this.statsMonitor.incRetriesExhausted();
			log.error("Error max retries reached for "+prevFutureContext.url);
			return;
		}
		Future<IASResponse> future= this.executor.submit(new CBProcessUrl(key) );
		FutureContext fctx= new FutureContext(key, future);
		fctx.setRetries(prevFutureContext.getRetries()+1);
		futureContextMap.put(key,fctx);
		this.statsMonitor.incResubmit();
	}

	private int read_urls() {
		Pattern pattern = Pattern.compile(this.urlFileRegEx);
		List<File> urlFiles = new LinkedList<File>();
		int ret = match(new File(this.urlDir), pattern, urlFiles);
		if(ret==ErrorCode.ERROR_SUCCESS){
    	   for(File urlFile : urlFiles){
    		   read_url_file(urlFile);
    	   }
		}
		this.urlCount = this.urlList.size();
		if(this.urlCount==0){
			ret=ErrorCode.ERROR_DATA_NOT_AVAILABLE;
		}
		return ret;
	}

	private void read_url_file(File urlFile) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(urlFile));
			String line=null;
			
			while((line=br.readLine())!=null){
				String fields[] = line.split(IASConstants.URL_FILE_DELIMITER);
				int cnt=0;
				String url = fields[cnt++].toLowerCase();
				this.urlList.add(url);
			}
			
			br.close();
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	static int match(File dir, Pattern pattern, List<File> matching) {
        File[] files = dir.listFiles();
        if(files==null) {
            log.error(dir + " is not a dir!");
            return ErrorCode.ERROR_EXCEPTION;
        }
        for (File file : files){
            //if (file.isDirectory()) match(file, pattern, matching);
            if (file.isFile()) {
            	String name = file.getName();
                Matcher matcher = pattern.matcher(name);
                if (matcher.matches()) {
                    matching.add(file);
                    log.error("url file "+file.getName());
                }
            }
        }
        return ErrorCode.ERROR_SUCCESS;
    }
	public static void main( String args[] ) throws Exception {
		if( args.length < 4 ) {
			System.out.println("Usage: Main -iap bin/res/server.properties -il4jp bin/res/log4j.properties -url_dir bin/res/test_urls.txt ");
			return;
		}
		Hashtable<String, String> params = com.sharethis.common.parser.CommandLineParser.parseCommandParams(args);
		
		AppConfig config = new AppConfig();
		config.init(params);
		
		ConfigHelper.replaceAppTokens(config, params);
		
		long startDate= System.currentTimeMillis();
		//read urls into list
		
		final IASDownloadServer server = new IASDownloadServer();
		int ret=ErrorCode.ERROR_SUCCESS;
		try{
			server.init(config, params);
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					//Send out shutdown notification
					Logger.getLogger(IASDownloadServer.class.getName()).info("IASDownloadServer shutdown !");
					server.preShutDown();
				}

			});
			ret=server.processUrls();
			if(ret==ErrorCode.ERROR_SUCCESS){
				log.error("Success: IAS download done!");
			}else{
				log.error("Failure: IAS download done!");

			}
		}catch(Exception e){

		}finally{
			server.shutdown();
			//server.join();
			long endDate = System.currentTimeMillis();
			log.error("Url Count="+server.urlCount+", Duration "+(endDate-startDate)/1000+" secs");
			int exitStatus=0;
			if(ret!=ErrorCode.ERROR_SUCCESS){
				exitStatus=-1;
			}
			System.exit(exitStatus);
		}
		
	}

	protected void preShutDown() {
		log.error("IASDownloadServer:preShutDown");
		//shutdown();
	}

	private void shutdown() {
		log.error("IASDownloadServer:ShutDown");
		this.statsMonitor.stop();
		this.executor.shutdown();
		this.throttleService.shutdown();
	}

}
