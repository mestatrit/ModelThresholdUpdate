package com.sharethis.tools.memcacheTest;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.OperationTimeoutException;

import com.sharethis.common.error.ErrorCode;
import com.sharethis.common.helper.log.LogHelper;

public class MemcachedPool {
	protected Logger log = Logger.getLogger(getClass());
	public enum ErrorType{
		Success,
		CancellationException,
		InterruptedException,
		ExecutionException,
		TimeoutException,
		OperationTimeoutException,
		IllegalStateException
	}
	Counters counters = new Counters();
	int num_clients;
	boolean isEnabled;

	private String host;
	//ConcurrentLinkedQueue<MemcachedClient> clientQueue=new ConcurrentLinkedQueue<MemcachedClient>();
	MemcachedClient[] clientArray;

	private long memcache_timeout;
	public MemcachedPool() {

	}

	public int init(String memcache_host,int num_memcache_threads, long timeout) {
		this.num_clients = 	num_memcache_threads;
		this.host=memcache_host;
		this.memcache_timeout = timeout;
		
		int ret=ErrorCode.ERROR_SUCCESS;
		clientArray = new MemcachedClient[num_clients];
		for(int i=0; i<num_clients; i++){
			try {
				MemcachedClient memcachedClient = new MemcachedClient(net.spy.memcached.AddrUtil.getAddresses(host));
				clientArray[i]=memcachedClient;
			} catch (IOException e) {
				ret=ErrorCode.ERROR_EXCEPTION;
				log.error("MemcachedPool Create MemcachedClient "+host+"\n"+LogHelper.formatMessage(e));
				break;
			}
		}
		return ret;
	}

	public Future<Object> asyncGet(String s) {
		long threadId = Thread.currentThread().getId();
		int index = getIndex(threadId);
		Future<Object> fut = clientArray[index].asyncGet(s);
		return fut;
	}

	private int getIndex(long id) { return (int)((id >> 32) ^ id) % this.num_clients; }

	public Object getResult(Future<Object> future) {
		Object value =null;
		try {
			value=(String)future.get(this.memcache_timeout, TimeUnit.MICROSECONDS);
			
		}catch(CancellationException e){
			counters.findCounter(ErrorType.CancellationException).increment(1);
		} catch (InterruptedException e) {
			counters.findCounter(ErrorType.InterruptedException).increment(1);
		} catch (ExecutionException e) {
			counters.findCounter(ErrorType.ExecutionException).increment(1);
		} catch (TimeoutException e) {
			counters.findCounter(ErrorType.TimeoutException).increment(1);
		} 
		if(value!=null) counters.findCounter(ErrorType.Success).increment(1);	
		return value;
	}
	public String getCounters(){ return this.counters.toString(); }

	public void setTimeout(long t) { this.memcache_timeout = t;}

	public Object get(String s) {
		long threadId = Thread.currentThread().getId();
		int index = getIndex(threadId);
		Object obj=null;
		try{
			obj=clientArray[index].get(s);
		}catch(OperationTimeoutException e){
			obj=null;
			counters.findCounter(ErrorType.OperationTimeoutException).increment(1);
		}catch(IllegalStateException e){
			obj=null;
			counters.findCounter(ErrorType.IllegalStateException).increment(1);
		}
		return obj;
	}
}
