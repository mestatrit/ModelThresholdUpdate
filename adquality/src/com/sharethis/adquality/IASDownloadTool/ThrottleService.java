package com.sharethis.adquality.IASDownloadTool;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.sharethis.common.error.ErrorCode;

/*
 * author: wahid chrabakh
 */

public class ThrottleService {
	static Logger log = Logger.getLogger(ThrottleService.class.getName());
	
	int reqCountPerPeriod;
	int reqPeriod;
	boolean isRunning=true;
	Thread periodicThread=null;
	AtomicInteger currCountPerPeriod=new AtomicInteger();
	private long periodStart;
	public ThrottleService(){}
	public int init(int reqCnt, int period){
		this.reqCountPerPeriod = reqCnt;
		this.reqPeriod = period;
		return ErrorCode.ERROR_SUCCESS;
	}
	public void shutdown(){ isRunning=false; }
	public void start(){
		periodicThread = new Thread (){
			public void run(){
				while(isRunning){
					try {
						sleep(ThrottleService.this.reqPeriod);
						ThrottleService.this.periodStart=System.currentTimeMillis();
						ThrottleService.this.reset();
					} catch (InterruptedException e) { }
				}
			}
		};
		ThrottleService.this.periodStart=System.currentTimeMillis();
		periodicThread.start();
	}
	void reset(){ currCountPerPeriod.set(0); }
	public boolean submitEvent(boolean sleepIfOverlimit){
		int reqCount =currCountPerPeriod.incrementAndGet();
		if(reqCount > reqCountPerPeriod){
			if(sleepIfOverlimit){
				try {
					long now= System.currentTimeMillis();
					long timePassed = now - this.periodStart;
					long timeLeft=1;
					if(timePassed>0 && timePassed<this.reqPeriod){
						//sleep for the duration of time left.
						timeLeft=this.reqPeriod-timePassed;
					}
						
					log.info("submitEvent: Sleep "+(this.reqPeriod - timePassed));
					Thread.sleep(timeLeft);//sleep for 1 msec to avoid empty loops on client side
				} catch (InterruptedException e) { }
			}
			log.info("submitEvent: Cannot submit!");
			return false;
		}
		return true;
	}
}
