package com.sharethis.adoptimization.campaigndata;

import java.util.HashMap;
import java.util.Map;

public class HourlyDataImpressionObject
{	
 	private String ip = null;
 	private String agid = null;
 	private String dmn = null;
 	private String cpid = null;
 	private String rdm = null;
 	private String cty = null;
 	private String ctw = null;
 	private String st = null;
 	private String src = null;
 	private String jid = null;
 	private String ctry = null;
 	private String lat = null;
 	private String date = null;
 	private String ctid = null;
 	private String asid = null;
 	private String zip = null;
 	private String lon = null;
 	private String ts = null;
 	private String cpnm = null;
 	private String dma = null;
 	private String cth = null;
 	private String cookie = null;
 	private String googid = null;
 	private String usragnt = null;
 	private String scpid = null;
 	private String sagid = null;
 	private String sctid = null;
 	private int errflg = 0;
 	private String flg = null;
 	private String did = null;
 	private String gfid = null;
 	private String devlat = null;
 	private String devlon = null;
 	
	public Map<String, String> toMap(){
 		Map<String, String> dMap = new HashMap<String, String>();
 		dMap.put("agid",agid);
 		dMap.put("ts",ts);
 		dMap.put("dmn",dmn);
 		dMap.put("cpid",cpid);
 		dMap.put("date",date);
 		dMap.put("sctid",sctid);
 		dMap.put("rdm",rdm);
 		dMap.put("cty",cty);
 		dMap.put("googid",googid);
 		dMap.put("ctw",ctw);
 		dMap.put("jid",jid);
 		dMap.put("st",st);
 		dMap.put("src",src);
 		dMap.put("ctry",ctry);
 		dMap.put("lat",lat);
 		dMap.put("ctid",ctid);
 		dMap.put("asid",asid);
 		dMap.put("usragnt",usragnt);
 		dMap.put("zip",zip);
 		dMap.put("lon",lon);
 		dMap.put("scpid",scpid);
 		dMap.put("cpnm",cpnm);
 		dMap.put("dma",dma);
 		dMap.put("did",did);
 		dMap.put("cth",cth);
 		dMap.put("sagid",sagid);
 		dMap.put("ip",ip);
 		dMap.put("cookie",cookie); 		
 		dMap.put("errflg",Integer.toString(errflg));
 		dMap.put("flg",flg);
 		dMap.put("gfid",gfid);
 		dMap.put("devlat",devlat);
 		dMap.put("devlon",devlon);
 		
 		return dMap;
 	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getAgid() {
		return agid;
	}

	public void setAgid(String agid) {
		this.agid = agid;
	}

	public String getDmn() {
		return dmn;
	}

	public void setDmn(String dmn) {
		this.dmn = dmn;
	}

	public String getCpid() {
		return cpid;
	}

	public void setCpid(String cpid) {
		this.cpid = cpid;
	}

	public String getRdm() {
		return rdm;
	}

	public void setRdm(String rdm) {
		this.rdm = rdm;
	}

	public String getCty() {
		return cty;
	}

	public void setCty(String cty) {
		this.cty = cty;
	}

	public String getCtw() {
		return ctw;
	}

	public void setCtw(String ctw) {
		this.ctw = ctw;
	}

	public String getSt() {
		return st;
	}

	public void setSt(String st) {
		this.st = st;
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

	public String getCtry() {
		return ctry;
	}

	public void setCtry(String ctry) {
		this.ctry = ctry;
	}

	public String getLat() {
		return lat;
	}

	public void setLat(String lat) {
		this.lat = lat;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getCtid() {
		return ctid;
	}

	public void setCtid(String ctid) {
		this.ctid = ctid;
	}

	public String getAsid() {
		return asid;
	}

	public void setAsid(String asid) {
		this.asid = asid;
	}

	public String getZip() {
		return zip;
	}

	public void setZip(String zip) {
		this.zip = zip;
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

	public String getCpnm() {
		return cpnm;
	}

	public void setCpnm(String cpnm) {
		this.cpnm = cpnm;
	}

	public String getDma() {
		return dma;
	}

	public void setDma(String dma) {
		this.dma = dma;
	}

	public String getCth() {
		return cth;
	}

	public void setCth(String cth) {
		this.cth = cth;
	}

	public String getCookie() {
		return cookie;
	}

	public void setCookie(String cookie) {
		this.cookie = cookie;
	}

	public String getGoogid() {
		return googid;
	}

	public void setGoogid(String googid) {
		this.googid = googid;
	}

	public String getUsragnt() {
		return usragnt;
	}

	public void setUsragnt(String usragnt) {
		this.usragnt = usragnt;
	}

	public String getScpid() {
		return scpid;
	}

	public void setScpid(String scpid) {
		this.scpid = scpid;
	}

	public String getSagid() {
		return sagid;
	}

	public void setSagid(String sagid) {
		this.sagid = sagid;
	}

	public String getSctid() {
		return sctid;
	}

	public void setSctid(String sctid) {
		this.sctid = sctid;
	}

	public int getErrflg() {
		return errflg;
	}

	public void setErrflg(int errflg) {
		this.errflg = errflg;
	}

	public String getFlg() {
		return flg;
	}

	public void setFlg(String flg) {
		this.flg = flg;
	}

	public String getDid() {
		return did;
	}

	public void setDid(String did) {
		this.did = did;
	}
	
 	public String getGfid() {
		return gfid;
	}

	public void setGfid(String gfid) {
		this.gfid = gfid;
	}

	public String getDevlat() {
		return devlat;
	}

	public void setDevlat(String devlat) {
		this.devlat = devlat;
	}

	public String getDevlon() {
		return devlon;
	}

	public void setDevlon(String devlon) {
		this.devlon = devlon;
	}
}
