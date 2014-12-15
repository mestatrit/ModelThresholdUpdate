package com.sharethis.adquality.IASReport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.IllegalFormatConversionException;
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

import org.apache.log4j.Logger;

import com.google.gson.JsonSyntaxException;
import com.sharethis.adquality.IASDownloadTool.EnvConstants;
import com.sharethis.adquality.IASDownloadTool.IASConstants;
import com.sharethis.adquality.IASDownloadTool.StatsMonitor;
import com.sharethis.adquality.IASDownloadTool.ThrottleService;
import com.sharethis.adquality.common.ConfigHelper;
import com.sharethis.common.constant.Constants;
import com.sharethis.common.error.ErrorCode;
import com.sharethis.common.helper.DBConnectionHelper;
import com.sharethis.common.helper.RuntimeHelper;
import com.sharethis.common.helper.app.AppConfig;
import com.sharethis.common.helper.log.LogHelper;
import com.sharethis.common.sql.DBConnect;
import com.sharethis.iasapi.IasCampaignViewabiltyReport;

public class ReportServer {
	static Logger log = Logger.getLogger(ReportServer.class.getName());

	private String adgroup_list_file;
	private String report_file;
	private int maxIasRetries=0;
	private int urlConnectionSize;
	private String iasHost;
	private int iasPort;
	private String iasCallFormat;
	private int iasTimeout;
	private boolean httpKeepAlive;
	private int httpMaxConnections;
	private Map<ReportInfo, FutureContext> futureContextMap = new HashMap<ReportInfo, FutureContext>();
	private int iasMaxRequestPerPeriod;
	private int iasPeriod;
	private Set<ReportInfo> siSet = new HashSet<ReportInfo>();
	private List<AdGroup> adGroupList = new LinkedList<AdGroup>();

	private String ias_authentication_url;

	private String ias_password;

	private String ias_username;

	private AuthenticatedHttpURLConnection authenticatedHttpURLConnection;

	private String reportCount;

	private int MaxOutstandingCalls;

	private ExecutorService executor;

	private ThrottleService throttleService = new ThrottleService();;

	private StatsMonitor statsMonitor= new StatsMonitor();

	private String viewab_report_url;

	private String brand_safety_report_url;

	private String ias_team_id;

	private int start_step=0;
	
	enum RUN_STEP{
		LOAD_FROM_DB,
		GET_REPORTS,
		UPLOAD_TO_DB
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
		final ReportServer server = new ReportServer();
		server.init(config, params);
		if(server.start_step <= RUN_STEP.LOAD_FROM_DB.ordinal()){
			server.writeAdGroupFile(server.adgroup_list_file, config);
		}
		int ret=ErrorCode.ERROR_SUCCESS;
		if(server.start_step <= RUN_STEP.GET_REPORTS.ordinal()){
			//1.Get AdGroup list from file
			if(server.getViewableAdGroups(server.adgroup_list_file)!=ErrorCode.ERROR_SUCCESS){
				log.error("Error uploading adgroup list from file "+ server.adgroup_list_file);
				return;
			}

			//2.Get Reports

			//2.A authenticate
			if(server.authenticate()!=ErrorCode.ERROR_SUCCESS){
				log.error("Error authenticating !!");
				return;
			}
			//2.B download reports
			try{
				Runtime.getRuntime().addShutdownHook(new Thread() {
					public void run() {
						//Send out shutdown notification
						Logger.getLogger(ReportServer.class.getName()).info("ReportServer shutdown !");
						server.preShutDown();
					}

				});
				ret=server.getReports();
				if(ret==ErrorCode.ERROR_SUCCESS){
					log.error("Success: IAS download done!");
				}else{
					log.error("Failure: IAS download done!");

				}
			}catch(Exception e){
				ret=ErrorCode.ERROR_GENERAL;
				log.error(LogHelper.formatMessage(e));
			}finally{
				server.shutdown();
				//server.join();
				server.statsMonitor.writeStats();
				long endDate = System.currentTimeMillis();
				log.error("Report Count="+server.reportCount+", Duration "+(endDate-startDate)/1000+" secs");


			}
		}
		if(server.start_step <= RUN_STEP.UPLOAD_TO_DB.ordinal()){
			server.update_db(config);
		}
		int exitStatus=0;
		if(ret!=ErrorCode.ERROR_SUCCESS){
			exitStatus=-1;
		}
		System.exit(exitStatus);

	}
	DBConnect getConnection(AppConfig config) {
		DBConnect conn = null;
		try {
			conn = DBConnectionHelper.createConnection(config);
		}
		catch(Exception e) {
			log.error(LogHelper.formatMessage(e));
		}		
		return conn;
	}
	private int update_db(AppConfig config) {
		int ret=ErrorCode.ERROR_SUCCESS;
		BufferedReader br = null;
		DBConnect conn = null;
		try {
			//Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			//String date = formatter.format(new Date());
			java.sql.Date now = new java.sql.Date(System.currentTimeMillis());
			conn =  getConnection(config);
			conn.setAutoCommit(false);
			Connection conObj = conn.getConnObj();
			PreparedStatement prepStmt = conObj.prepareStatement(    
			   //"INSERT INTO AdQualityByAdGroup (adgid,viewability,updateDate) VALUES (?,?,?) ON DUPLICATE KEY UPDATE viewability=?, updateDate=? ;"
			   "INSERT INTO AdQualityByAdGroup (adgid,viewability,updateDate) VALUES (?,?,?) ON DUPLICATE KEY UPDATE viewability=?, updateDate=? ;"
			);
			//insert into AdQualityByAdGroup (adgid,viewability,date) VALUES (?,?,?) ON DUPLICATE KEY UPDATE viewability=? date=?;
			
			br = new BufferedReader(new FileReader(this.report_file));
			String line=null;
			while((line=br.readLine())!=null){
				String[] fields=line.split("\t");
				int cnt=0;
				long adgid = Long.parseLong(fields[cnt++]);
				String metric = fields[cnt++];
				String json = fields[cnt++];
				try {
					IasCampaignViewabiltyReport iasReport = IasCampaignViewabiltyReport.fromJson(json);
					float inView1sPct=Float.parseFloat(iasReport.viewability.get(0).inView1sPct);
					int index=1;
					prepStmt.setLong(index++,adgid);
					prepStmt.setFloat(index++,inView1sPct);
					prepStmt.setDate(index++,now);
					prepStmt.setFloat(index++,inView1sPct);
					prepStmt.setDate(index++,now);
					prepStmt.addBatch(); 
				} catch (SQLException e) {
					log.error(LogHelper.formatMessage(e));
				} catch (JsonSyntaxException e){
					log.error("Report: "+json);
					log.error(LogHelper.formatMessage(e));
				}
			}
			int[] numUpdates = prepStmt.executeBatch();
			
			log.error("Num updates = "+numUpdates.length);
			for (int i=0; i < numUpdates.length; i++) {            
				if (numUpdates[i] == -2)
					log.error("Execution " + i + 
							": unknown number of rows updated");
				else
					log.error("Execution " + i + 
							"successful: " +numUpdates[i] + " rows updated");
			}

			conObj.commit();
		} catch (FileNotFoundException e) {
			log.error(LogHelper.formatMessage(e));
		} catch (IOException e) {
			log.error(LogHelper.formatMessage(e));
		} catch (SQLException e) {
			log.error(LogHelper.formatMessage(e));
		}finally{
			if(br!=null){
				try {
					br.close();
				} catch (IOException e) { log.error(LogHelper.formatMessage(e)); }
			}
			if(conn!=null) {
				try {
					conn.doClose();
				} catch (SQLException e) { log.error(LogHelper.formatMessage(e)); }
		
			}
		}
		return ret;
	}

	private int authenticate() {
		
		this.authenticatedHttpURLConnection = new AuthenticatedHttpURLConnection(this.ias_authentication_url, this.ias_username, this.ias_password,this.iasTimeout);
		return this.authenticatedHttpURLConnection.authenticate();
	}
	
	

	private void writeAdGroupFile(String adgoup_list_file, AppConfig config) {
		AdGroupFileWriter adgw = new AdGroupFileWriter();
		//adgw.init(config);
		try {
			adgw.write(adgoup_list_file, config);
		} catch (Exception e) {
			log.error("writeAdGroupFile:"+adgoup_list_file+"\n"+LogHelper.formatMessage(e));
		}
		
	}

	public class FutureContext {
		private ReportInfo ri;
		private Future<IASResponse> future;
		private int retries=0;
		
		public FutureContext(ReportInfo ri, Future<IASResponse> future) {
			this.ri=ri;
			this.future=future;
		}

		public Future<IASResponse> getFuture() { return this.future; }
		public ReportInfo getReportInfop() { return ri; }
		public int getRetries() { return retries; }
		public void setRetries(int retries) { this.retries = retries; }
	}
	enum ReportType {
		Viewability("campaignviewability",0x1),
		BrandSafety("campaignsafety",0x10);
		String urlParam;
		int flag;
		ReportType(String param, int flag){ 
			this.urlParam = param;
			this.flag =flag;
		}
		public boolean isEnabled(int flags) { return ((this.flag & flags) >0); }
	}
	public class IASResponse {
		public IasCampaignViewabiltyReport iasReport;
		public String rawData="";
		
		ReportInfo reportInfo;
		public int respCode=0;
		public IASResponse(ReportInfo ri) {
			this.reportInfo = ri;
		}
		public boolean hasData() {
			return this.iasReport.recordCount!=0;//(iasData.uem.iviab!=null); 
		}
	}
	class CBProcess implements Callable<IASResponse> {
		ReportInfo ri;
		int retries=0;
		CBProcess(ReportInfo ri){
			this.ri=ri;
		}
		@Override
		public IASResponse call() throws Exception {
			IASResponse iasResp = new IASResponse(this.ri);
			String callUrl=null;
			try {
				if(this.ri.rt == ReportType.Viewability){
					callUrl=String.format(ReportServer.this.viewab_report_url,ReportServer.this.ias_team_id, this.ri.adg.adGroupId,this.ri.rt.urlParam);
				}else{
					callUrl = String.format(ReportServer.this.brand_safety_report_url,ReportServer.this.ias_team_id,this.ri.adg.adGroupId,this.ri.rt.urlParam);
				}
				log.error("Request:"+callUrl);
				String response = ReportServer.this.authenticatedHttpURLConnection.getUrl(callUrl);
				log.error("Response:"+response);
				IasCampaignViewabiltyReport iasReport = IasCampaignViewabiltyReport.fromJson(response);
				iasResp.iasReport = iasReport;
			}catch(IllegalFormatConversionException e){
				log.error(LogHelper.formatMessage(e));
			}catch( com.google.gson.JsonSyntaxException e){
				log.error(LogHelper.formatMessage(e));
			}
			
			return iasResp;
		}
		
		void setRetries(int v) { this.retries=v;}
		int getRetries(){ return this.retries; }
	}
	class ReportInfo{
		public ReportInfo(AdGroup adg, ReportType rt) {
			this.adg=adg;
			this.rt=rt;
		}
		final AdGroup adg;
		ReportType rt;
		@Override
		public String toString(){
			return "("+adg.adGroupId+","+rt+")";
		}
		@Override
		public boolean equals(Object o) {
			if (o instanceof ReportInfo) {
				ReportInfo other = (ReportInfo) o;
				return adg.adGroupId == other.adg.adGroupId && rt.equals(other.rt);
			}
			return false;
		}

		@Override
		public int hashCode() {
			return new Long(adg.adGroupId).hashCode() * 37 + rt.hashCode();
		}
	}
	private int getReports() {
		Queue<ReportInfo> reportQueue = new ConcurrentLinkedQueue<ReportInfo>();
		for(AdGroup adg: this.adGroupList){
			if( ReportType.Viewability.isEnabled(adg.flags))
				reportQueue.add(new ReportInfo(adg, ReportType.Viewability));
		}
		this.statsMonitor.setTotal(reportQueue.size());
		//start progress thread
		//Open output file
		BufferedWriter bw=null;
		try {
			bw = new BufferedWriter(new FileWriter(this.report_file));
		} catch (IOException e) {
			log.error("Cannot find output file "+ this.report_file);
			log.error(LogHelper.formatMessage(e));
			return ErrorCode.ERROR_FILE_NOT_FOUND;
		}
		
		statsMonitor.start();
		while(!reportQueue.isEmpty() || !futureContextMap.isEmpty()){
			//log.error("URLQueue="+urlQueue.size()+", futureMap="+futureContextMap.size());
			ReportInfo ri=null;
			if(!reportQueue.isEmpty()){
				ri=reportQueue.remove();
			}
			if(ri != null){
				while(!this.throttleService.submitEvent(true)){
					log.info("Cannot submit");
				}
				this.statsMonitor.incSubmit();
				//log.error("Submitting...");

				Future<IASResponse> future= this.executor.submit(new CBProcess(ri) );
				FutureContext fctx= new FutureContext(ri, future);
				futureContextMap.put(ri,fctx);

			}
			if(reportQueue.isEmpty() || futureContextMap.size() >= this.MaxOutstandingCalls){
				processFutures(bw);
			}

		}
		if(bw!=null){
			try {
				bw.close();
			} catch (IOException e) { log.error(LogHelper.formatMessage(e)); }
		}
		return 0;
	}

	private void processFutures(BufferedWriter bw) {
		List<ReportInfo> completedKeys = new ArrayList<ReportInfo>();
		Map<ReportInfo, FutureContext> resubmitMap = new HashMap<ReportInfo, FutureContext>();
		for(Map.Entry<ReportInfo, FutureContext> entry: this.futureContextMap.entrySet()){
			ReportInfo key=entry.getKey(); 
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
						 if (iasResp.hasData()){
							this.siSet.add(iasResp.reportInfo);
							//only write si
							try {
								bw.write(iasResp.reportInfo.adg.adGroupId
										+ IASConstants.URL_FILE_DELIMITER+iasResp.reportInfo.rt
										+IASConstants.URL_FILE_DELIMITER+iasResp.iasReport.toJson(false)
										+"\n");
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}else{
							log.error("Empty IAS Report:"+futureContext.ri+ "->"+iasResp.iasReport.toJson(false));
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
		for(ReportInfo key:completedKeys ){
			this.futureContextMap.remove(key);
		}
		for(Map.Entry<ReportInfo, FutureContext> entry: resubmitMap.entrySet()){
			reSubmitReport(entry.getKey(), entry.getValue());
		}
	}
	
	private void reSubmitReport(ReportInfo key, FutureContext prevFutureContext) {
		if(prevFutureContext.getRetries()>= this.maxIasRetries){
			this.statsMonitor.incRetriesExhausted();
			log.error("Error max retries reached for "+prevFutureContext.ri);
			return;
		}
		log.error("Resubmit retry "+prevFutureContext.getRetries()+" for "+prevFutureContext.ri);
		Future<IASResponse> future= this.executor.submit(new CBProcess(key) );
		FutureContext fctx= new FutureContext(key, future);
		fctx.setRetries(prevFutureContext.getRetries()+1);
		futureContextMap.put(key,fctx);
		this.statsMonitor.incResubmit();
	}

	private int init(AppConfig config, Hashtable<String, String> params) throws IOException {
		int ret=-1;
		String il4jp = params.get(Constants.INPUT_LOG4J_PROPS);
		RuntimeHelper.setLog4J(il4jp, Constants.INPUT_LOG4J_PROPS_DEF);
		
		if(params.get(EnvConstants.START_STEP)!=null){
			ConfigHelper.copyParam(config,params.get(EnvConstants.START_STEP),EnvConstants.START_STEP);
			log.error("Overriding Param START_STEP="+RUN_STEP.values()[config.getInt(EnvConstants.START_STEP)]);
		}
		
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
		
		this.adgroup_list_file = config.get(IASConstants.ADGROUP_LIST_FILE);
		this.report_file =  config.get(IASConstants.REPORT_FILE);
		this.maxIasRetries = config.getInt(IASConstants.MAX_IAS_RETRIES);
		this.iasTimeout = config.getInt(IASConstants.IAS_TIMEOUT);
		//authentication
		this.ias_authentication_url = config.get(IASConstants.IAS_AUTHENTICATION_URL);
		this.ias_username = config.get(IASConstants.IAS_USERNAME);
		this.ias_password = config.get(IASConstants.IAS_PASSWORD);
		this.ias_team_id = config.get(IASConstants.IAS_TEAM_ID);
		//url
		this.viewab_report_url = config.get(IASConstants.VIEWAB_REPORT_URL);
		this.brand_safety_report_url = config.get(IASConstants.BRAND_SAFETY_REPORT_URL);
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
		this.executor= Executors.newFixedThreadPool(config.getInt(IASConstants.MAX_DL_THREADS));
		this.throttleService.init(this.iasMaxRequestPerPeriod, this.iasPeriod);
		this.throttleService.start();
		log.error("Throttle: period="+this.iasPeriod+", maxReqPerPeriod="+this.iasMaxRequestPerPeriod);
		//get keep alive
		log.error(IASConstants.HTTP_KEEP_ALIVE+":"+System.getProperty(IASConstants.HTTP_KEEP_ALIVE));
		log.error(IASConstants.HTTP_MAX_CONNECTIONS+":"+System.getProperty(IASConstants.HTTP_MAX_CONNECTIONS));
		//
		this.start_step = config.getInt(EnvConstants.START_STEP);
		
		return ret;
		
	}

	private int getViewableAdGroups(String adgouplistFile) {
		int ret=ErrorCode.ERROR_SUCCESS;
		this.adGroupList = AdGroupFileWriter.listFromFile(adgouplistFile);
		return ret;
		
	}

	protected void preShutDown() {
		log.error("ReportServer:preShutDown");
		//shutdown();
	}

	private void shutdown() {
		log.error("ReportServer:ShutDown");
		this.statsMonitor.stop();
		this.executor.shutdown();
		this.throttleService.shutdown();
	}
	
}
