package com.sharethis.tools.memcacheTest;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class MemcacheTest {
	Counters counters = new Counters();
	public enum CounterType{
		Success,
		InterruptedException,
		ExecutionException,
		TimeoutException, Error 
	}
	final static MetricRegistry metrics = new MetricRegistry();
	private final Histogram responseTimes = metrics.histogram(MemcacheTest.class+ ":response-time");
	private final Counter successCounter = metrics.counter(MemcacheTest.class+":success-counter");
	private final Counter failCounter = metrics.counter(MemcacheTest.class+":fail-counter");
	private final Meter qps = metrics.meter(MemcacheTest.class+":QPS");

	
	MemcachedPool memcachePool; 
	ExecutorService executorService;
	public long memcache_timeout;
	private int nThreads;
	private String memcache_host;
	private int num_memcache_threads;
	private boolean use_async;
	public int init(String memcache_host, int num_memcache_threads, int timeout, int nThreads, boolean use_async){
		int ret=0;
		this.memcache_timeout = timeout;
		this.nThreads= nThreads;
		this.memcache_host=memcache_host;
		this.num_memcache_threads = num_memcache_threads;
		this.use_async=use_async;
		memcachePool = new MemcachedPool();
		memcachePool.init(memcache_host, num_memcache_threads, timeout);
		executorService = Executors.newFixedThreadPool(nThreads); 
		return ret;
	}
	class UrlRequest {
		
	}
	public interface callMemcache {
		void process();
	}
	

	private class GetUrlInfoAsync implements Runnable, callMemcache{
		List<String> urlList;
		public GetUrlInfoAsync(List<String> ulist) {
			this.urlList = ulist;
		}
		@Override
		public void run() {
			while(!isDone()){
				process();
				
			}
			
		}
		private String processFuture(Future<Object> fut) {
			String s=null;
			try {
				s = (String)fut.get(MemcacheTest.this.memcache_timeout,TimeUnit.MICROSECONDS);
			} catch (InterruptedException e) {
				MemcacheTest.this.counters.findCounter(CounterType.InterruptedException).increment(1);
				e.printStackTrace();
			} catch (ExecutionException e) {
				MemcacheTest.this.counters.findCounter(CounterType.ExecutionException).increment(1);
				e.printStackTrace();
			} catch (TimeoutException e) {
				MemcacheTest.this.counters.findCounter(CounterType.TimeoutException).increment(1);
				//e.printStackTrace();
			}
			if(s!=null) MemcacheTest.this.counters.findCounter(CounterType.Success).increment(1);
			return s;
		}
		private boolean isDone() { return false; }
		@Override
		public void process() {
			for(String url: this.urlList){
				long startTime = System.nanoTime();
				Future<Object> fut=MemcacheTest.this.memcachePool.asyncGet(url);
				String s = processFuture(fut);
				qps.mark();
				responseTimes.update((System.nanoTime() - startTime)/1000);
				if(s!=null) successCounter.inc(); else failCounter.inc();
			}
		}
	}
	private class GetUrlInfo implements Runnable, callMemcache{
		List<String> urlList;
		public GetUrlInfo(List<String> ulist) {
			this.urlList = ulist;
		}
		@Override
		public void run() {
			while(!isDone()){
				process();
				
			}
			
		}
		private boolean isDone() { return false; }
		@Override
		public void process() {
			for(String url: this.urlList){
				long startTime = System.nanoTime();
				String s =(String)MemcacheTest.this.memcachePool.get(url);
				long endTime = System.nanoTime();
				qps.mark();
				responseTimes.update((System.nanoTime() - startTime)/1000);
				if(s!=null){
					successCounter.inc();
					MemcacheTest.this.counters.findCounter(CounterType.Success).increment(1);	
				}else{
					failCounter.inc();
					MemcacheTest.this.counters.findCounter(CounterType.Error).increment(1);	
				}
			}
		}
	}
	public interface Params {
		/*public final static String  THREAD_COUNT= "tc";
		public static final String MEMCACHE_HOST = "mh";
		public static final String MEMCACHE_NCLIENTS = "mnc";
		public static final String MEMCACHE_TIMEOUT = "mt";
		public static final String URL_FILE = "uf";
		*/
		public static final String PROP_FILE = "conf";
		public static final String LOG4J = "log4j";
		
	}
	public interface Props{
		public final static String THREAD_COUNT= "thread_count";
		public static final String MEMCACHE_HOST = "memcache_host";
		public static final String MEMCACHE_NCLIENTS = "memcache_nclients";
		public static final String MEMCACHE_TIMEOUT = "memcache_timeout";
		public static final String URL_FILE = "url_file";
		public static final String USE_ASYNC = "use_async";
		//public static final String PROP_FILE = "conf";
		//public static final String LOG4J = "log4j";
	}
	private static Logger log = Logger.getLogger(MemcacheTest.class);
	public static void main(String args [] ) throws Exception {
		
		Map<String,String> params = parseParams(args);
		printParams(params);
		
		String log4j = params.get(Params.LOG4J);
		Properties logProp = new Properties();      
	    logProp.load(new FileInputStream (log4j));  
	    PropertyConfigurator.configure(logProp);      
	    log.info("Logging enabled");    
	     
		String propFile = params.get(Params.PROP_FILE);
		Properties props = new Properties();
		props.load(new FileInputStream(propFile));
		
		
		
		int nThreads = Integer.parseInt(props.getProperty(Props.THREAD_COUNT));
		String memcache_host = props.getProperty(Props.MEMCACHE_HOST);
		int memcache_nclients = Integer.parseInt(props.getProperty(Props.MEMCACHE_NCLIENTS));
		int memcache_timeout = Integer.parseInt(props.getProperty(Props.MEMCACHE_TIMEOUT));
		String urlFile = props.getProperty(Props.URL_FILE);
		boolean use_async = Boolean.parseBoolean(props.getProperty(Props.USE_ASYNC));

		final JmxReporter reporter = JmxReporter.forRegistry(MemcacheTest.metrics).build();
		reporter.start();
		
		final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(MemcacheTest.metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
		consoleReporter.start(10, TimeUnit.SECONDS);

		MemcacheTest memcacheTest = new MemcacheTest();
		memcacheTest.init(memcache_host, memcache_nclients, memcache_timeout, nThreads, use_async);
		List<String> urlList = readUrlFile(urlFile);
		System.out.println("URLs="+urlList.size());
		memcacheTest.execute(urlList, nThreads);
		
	}
	public class StatsThread implements Runnable {
		boolean isDone=false;
		@Override
		public void run() {
			while(!isDone){
				MemcacheTest.this.printStats();
				log.error("Histogram:"+MemcacheTest.this.responseTimes.toString());
				try { Thread.sleep(10000); } catch(InterruptedException e){}
			}
			
		}
		
	}
	private void execute(List<String> urlList, int nThreads) {
		for(int i=0; i<nThreads-1; i++){
			if(this.use_async){
				executorService.execute(new GetUrlInfoAsync(urlList));
			}else{
				executorService.execute(new GetUrlInfo(urlList));
			}
		}
		executorService.execute(new StatsThread());
		
	}
	public void printStats() {
		log.error("Tester Stats:\n"+this.getCounters());
		long sCount = this.counters.findCounter(CounterType.Success).get();
		long eCount = this.counters.findCounter(CounterType.TimeoutException).get();
		float percent = 100.0f*sCount/(sCount+eCount);
		log.error("\t% Success : "+percent+"%");
		log.error("MemCache Stats:"+this.memcachePool.getCounters());
		
	}
	private String getCounters() { return counters.toString(); }
	private static void printParams(Map<String, String> params) {
		for(Map.Entry<String, String> e: params.entrySet()){
			System.out.println(e.getKey()+":"+e.getValue());
		}
		
	}
	private static List<String> readUrlFile(String urlFile) {
		BufferedReader br=null;
		List<String> urlList = new LinkedList<String>();
		try {
			br = new BufferedReader(new FileReader(urlFile));
			String line;
			try {
				while((line = br.readLine())!=null){
					String fields[] = line.split("\t");
					if(fields.length>0){
						urlList.add(fields[0]);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if (br!=null) br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return urlList;
	}
	private static Map<String,String> parseParams(String[] args) throws Exception {
		Map<String,String> params= new HashMap<String,String>();
		for (int i=0; i< args.length; i++){
			String p = args[i];
			if(p.startsWith("-")){
				String value = args[++i];
				params.put(p.substring(1), value);
			}else{
				throw new Exception("Bad parameters");
			}
		}
		return params;
	}
	/*private Future<Object> asyncGet(String d) {
		Future<Object> fut=this.memcachedClient.asyncGet(d);
		return fut;
	}
	private String get(String d) {
		return (String)this.memcachedClient.get(d);
	}*/

}
