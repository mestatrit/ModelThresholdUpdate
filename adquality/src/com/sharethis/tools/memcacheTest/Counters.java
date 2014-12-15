package com.sharethis.tools.memcacheTest;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.sharethis.tools.memcacheTest.Counters.Counter;

public class Counters {
	public static class Counter{
		AtomicLong count=new AtomicLong();
		public long increment(int i) { return count.addAndGet(i); }
		public long get(){return count.get();}
		public String toString(){ return count.toString();}
	}
	Map<Enum<?>, Counter> counterMap = new ConcurrentHashMap<Enum<?>, Counter>();
	public Counter findCounter(Enum<?> key) {
		Counter counter = counterMap.get(key);
		if(counter==null){
			counter = new Counter();
			counterMap.put(key,counter);
		}
		return counter;
	}
	public String toString(){
		StringBuilder sb = new StringBuilder();
		Set<Entry<Enum<?>, Counter>> set=this.counterMap.entrySet();
		Iterator<Entry<Enum<?>, Counter>> itr = set.iterator();
		if(itr.hasNext()){
			Entry<Enum<?>, Counter> entry = itr.next();
			sb.append("\t"+entry.getKey().toString()+" : "+entry.getValue().toString());
		}
		while(itr.hasNext()){
			Entry<Enum<?>, Counter> entry = itr.next();
			sb.append("\n\t"+entry.getKey().toString()+" : "+entry.getValue().toString());
		}
		return sb.toString();
	}

}
