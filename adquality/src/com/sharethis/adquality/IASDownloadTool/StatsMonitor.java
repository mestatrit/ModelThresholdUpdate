package com.sharethis.adquality.IASDownloadTool;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.sharethis.common.metrics.Counters;

public class StatsMonitor {
	static Logger log = Logger.getLogger(StatsMonitor.class.getName());
	Counters counters = new Counters();
	public enum StatCounters{
		Urls,
		Submits,
		Dones,
		Resubmits,
		ExhaustedRetries,
		RepeatedSi,
		NoVisData
	}
	class ServerStats{
		
		public String writeCounts() {
			return "total="+counters.findCounter(StatCounters.Urls).get()
					+ ", submit="+counters.findCounter(StatCounters.Submits).get()
					+ " , done="+counters.findCounter(StatCounters.Dones).get()
					+ " ,resubmit="+counters.findCounter(StatCounters.Resubmits).get()
					+ " ,retriesExhausted="+counters.findCounter(StatCounters.ExhaustedRetries).get();
		}
		public String writePercents() {
			counters.toStringPercent(StatCounters.Submits, StatCounters.Urls);
			return "Percents: submit="+counters.toStringPercent(StatCounters.Submits, StatCounters.Urls)
					+", done="+counters.toStringPercent(StatCounters.Dones, StatCounters.Submits)
					+",resubmit="+counters.toStringPercent(StatCounters.Resubmits, StatCounters.Submits)
					+", restriesExhausted="+counters.toStringPercent(StatCounters.ExhaustedRetries, StatCounters.Submits)
					+", RepeatedSi="+counters.toStringPercent(StatCounters.RepeatedSi, StatCounters.Submits)
					+", NoVisData="+counters.toStringPercent(StatCounters.NoVisData, StatCounters.Submits);
		}
	}
	ServerStats serverStats = new ServerStats();
	boolean running=false;
	int monitorPeriod=10000;
	private long startTime;
	
	public void setTotal(long i) {counters.findCounter(StatCounters.Urls).set(i);}
	public void incDoneCount() {counters.findCounter(StatCounters.Dones).increment(1);}
	public void incSubmit() {counters.findCounter(StatCounters.Submits).increment(1);}
	public void incResubmit() {counters.findCounter(StatCounters.Resubmits).increment(1);}
	public void incRetriesExhausted() {counters.findCounter(StatCounters.ExhaustedRetries).increment(1); }
	public void incRepeatedSi(){counters.findCounter(StatCounters.RepeatedSi).increment(1);}
	public void incNoVisData(){counters.findCounter(StatCounters.NoVisData).increment(1);}
	
	public void start(){
		this.running=true;
		this.startTime= System.currentTimeMillis();
		new Thread(){
			@Override
			public void run(){
			while(running){
				StatsMonitor.this.writeStats();
				try { Thread.sleep(monitorPeriod); } catch (InterruptedException e) { }
			}
			log.error("Stats Monitor thread exit!");
			}
		}.start();
	}
	public void writeStats() {
		long dur = System.currentTimeMillis()-this.startTime;
		log.error("Time="+(dur/1000)+", QPS="+(1000f*counters.findCounter(StatCounters.Dones).get()/dur)+", "+serverStats.writePercents());
	}
	public void stop(){
		running=false;
	}
}
