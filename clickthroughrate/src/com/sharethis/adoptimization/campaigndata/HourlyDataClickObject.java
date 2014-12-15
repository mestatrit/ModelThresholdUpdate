package com.sharethis.adoptimization.campaigndata;

import java.util.HashMap;
import java.util.Map;

public class HourlyDataClickObject
{	

	private String ip=null;
	private String lon=null;
	private String ts=null;
	private String dmn=null;
	private String dma=null;
	private String cookie=null;
	private String usragnt=null;
	private String url=null;
	private String src=null;
	private String jid=null;
	private String lat=null;
	private String ctry=null;
	private String date=null;
	private String cty=null;
	private String st=null;
	private String log=null;
	private String zip=null;
	private String aid=null;
	private String cid=null;
	
	public Map<String, String> toMap(){
		Map<String, String> dMap = new HashMap<String, String>();
		dMap.put("usragnt", usragnt);
		dMap.put("lon", lon);
		dMap.put("ts", ts);
		dMap.put("dmn", dmn);
		dMap.put("dma", dma);
		dMap.put("date", date);
		dMap.put("url", url);
		dMap.put("ip", ip);
		dMap.put("cookie", cookie);
		dMap.put("cty", cty);
		dMap.put("jid", jid);
		dMap.put("st", st);
		dMap.put("src", src);
		dMap.put("lat", lat);
		dMap.put("ctry", ctry);
		dMap.put("log", log);
		dMap.put("zip", zip);
		dMap.put("aid", aid);
		dMap.put("cid", cid);
		
		return dMap;		
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getLon() {
		return lon;
	}

	public void setLon(String lon) {
		this.lon = lon;
	}

	public String getTs() {
		return ts;
	}

	public void setTs(String ts) {
		this.ts = ts;
	}

	public String getDmn() {
		return dmn;
	}

	public void setDmn(String dmn) {
		this.dmn = dmn;
	}

	public String getDma() {
		return dma;
	}

	public void setDma(String dma) {
		this.dma = dma;
	}

	public String getCookie() {
		return cookie;
	}

	public void setCookie(String cookie) {
		this.cookie = cookie;
	}

	public String getUsragnt() {
		return usragnt;
	}

	public void setUsragnt(String usragnt) {
		this.usragnt = usragnt;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getSrc() {
		return src;
	}

	public void setSrc(String src) {
		this.src = src;
	}

	public String getJid() {
		return jid;
	}

	public void setJid(String jid) {
		this.jid = jid;
	}

	public String getLat() {
		return lat;
	}

	public void setLat(String lat) {
		this.lat = lat;
	}

	public String getCtry() {
		return ctry;
	}

	public void setCtry(String ctry) {
		this.ctry = ctry;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getCty() {
		return cty;
	}

	public void setCty(String cty) {
		this.cty = cty;
	}

	public String getSt() {
		return st;
	}

	public void setSt(String st) {
		this.st = st;
	}

	public String getLog() {
		return log;
	}

	public void setLog(String log) {
		this.log = log;
	}

	public String getZip() {
		return zip;
	}

	public void setZip(String zip) {
		this.zip = zip;
	}

	public String getAid() {
		return aid;
	}

	public void setAid(String aid) {
		this.aid = aid;
	}

	public String getCid() {
		return cid;
	}

	public void setCid(String cid) {
		this.cid = cid;
	}
	
}
