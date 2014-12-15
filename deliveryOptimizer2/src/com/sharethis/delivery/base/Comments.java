package com.sharethis.delivery.base;

import com.sharethis.delivery.util.StringUtils;

public class Comments {
	private int segmentCount;
	private String[] comments;

	public Comments(int segmentCount) {
		this.segmentCount = segmentCount;
		this.comments = new String[segmentCount];
		set("");
		
	}
	
	public void set(String msg) {
		for (int i = 0; i < segmentCount; i++) {
			comments[i] = msg;
		}
	}
	
	public void add(int p, String msg) {
		comments[p] = StringUtils.concat(comments[p], msg);
	}
	
	public void add(String msg) {
		for (int p = 0; p < segmentCount; p++) {
			add(p, msg);
		}
	}
	
	public void addGoalComments(boolean conversionTargetMet, boolean impressionTargetMet) {
		if (conversionTargetMet && impressionTargetMet) {
			add("Conversion and impression goals met");
		} else if (conversionTargetMet) {
			add("Conversion goal met");
		} else if (impressionTargetMet) {
			add("Impression goal met");
		}
	}
	
	public String get(int p) {
		return comments[p];
	}
}
