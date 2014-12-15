package com.sharethis.adoptimization.brandlift.parsing;

import java.util.HashMap;
import java.util.Map;

public class BrandLiftEventDataObject
{	

// RetargEventData for Brand Lift Campaign
/*
 {"evnttyp":"retarg",
"cookie":"31A2A00AD43C0F545812623B02074103",
"raw":"[09/Sep/2014:18:57:32 +0000]\tGET /socialOptimizationPixel.php?campaign=0OP&site=sharethis&ctvid=300x250&answerid=[answer_id]&answervalue=[answer_value]&jid=410316ba-943e-4d2c-8717-dcca277609e1 HTTP/1.1\thttp://shasta.vizu.com/cdn/00/01/41/23/3pt/127606/a5.htm?pid=127606&vzcid=14123&adtype=1&vzadid=300x250&ttype=7&vzsid=sharethis&siteurl=news.kron4.com&vzplid=&vzwc=&vzc1=&vzc2=&vzc3=&vzc4=&vzc5=&vzc6=410316ba-943e-4d2c-8717-dcca277609e1&vzc7=&vzc8=&vzc9=&vzc10=&iframe=0&vzwt=&vzsize=&vztc=&visible=1&_t=1057716768&preexp=1&postexp=1&impcnt=3&uid=A95A396F-CF29-B2A7-0EB9-B00DEEE7523F.1410288112&aid=919586&t=1410289052416\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; MS-RTC LM 8)\t 72.34.128.250\t-\t__stid=CqCiMVQPPNQ7YhJYA0EHAg==",
"cmpn":"0OP",
"brwsos":"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; MS-RTC LM 8)",
"date":"20140909 18:57:32",
"url":"http://shasta.vizu.com/cdn/00/01/41/23/3pt/127606/a5.htm?pid=127606&vzcid=14123&adtype=1&vzadid=300x250&ttype=7&vzsid=sharethis&siteurl=news.kron4.com&vzplid=&vzwc=&vzc1=&vzc2=&vzc3=&vzc4=&vzc5=&vzc6=410316ba-943e-4d2c-8717-dcca277609e1&vzc7=&vzc8=&vzc9=&vzc10=&iframe=0&vzwt=&vzsize=&vztc=&visible=1&_t=1057716768&preexp=1&postexp=1&impcnt=3&uid=a95a396f-cf29-b2a7-0eb9-b00deee7523f.1410288112&aid=919586&t=1410289052416",
"rttyp":"socialoptimizationpixel",
"ip":"72.34.128.250"}	
 
  	public static final String[] retargJsonNames = new String[]{"evnttyp","cookie",
 		"cmpn","brwsos","date","url","rttyp","ip","zip","cty","st","lon","lan","dma","ctry"};

 */
	private String evnttyp=null;
	private String cookie=null;
	private String cmpn=null;
	private String brwsos=null;
	private String date=null;
	private String url=null;
	private String rttyp=null;
	private String ip=null;
	private String zip=null;
	private String cty=null;
	private String st=null;
	private String lon=null;
	private String lan=null;
	private String dma=null;
	private String ctry=null;
			
	public Map<String, String> toMap(){
		Map<String, String> dMap = new HashMap<String, String>();
		dMap.put("evnttyp", evnttyp);
		dMap.put("cookie", cookie);
		dMap.put("cmpn", cmpn);
		dMap.put("brwsos", brwsos);
		dMap.put("date", date);
		dMap.put("url", url);
		dMap.put("rttyp", rttyp);
		dMap.put("ip", ip);
		dMap.put("zip", zip);
		dMap.put("cty", zip);
		dMap.put("st", st);
		dMap.put("lon", lon);
		dMap.put("lan", lan);
		dMap.put("dma", dma);
		dMap.put("ctry", ctry);
		
		return dMap;		
	}

	public String getEvnttyp() {
		return evnttyp;
	}

	public void setEvnttyp(String evnttyp) {
		this.evnttyp = evnttyp;
	}

	public String getCookie() {
		return cookie;
	}

	public void setCookie(String cookie) {
		this.cookie = cookie;
	}

	public String getCmpn() {
		return cmpn;
	}

	public void setCmpn(String cmpn) {
		this.cmpn = cmpn;
	}

	public String getBrwsos() {
		return brwsos;
	}

	public void setBrwsos(String brwsos) {
		this.brwsos = brwsos;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getRttyp() {
		return rttyp;
	}

	public void setRttyp(String rttyp) {
		this.rttyp = rttyp;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getZip() {
		return zip;
	}

	public void setZip(String zip) {
		this.zip = zip;
	}

	public String getSt() {
		return st;
	}

	public void setSt(String st) {
		this.st = st;
	}

	public String getCty() {
		return cty;
	}

	public void setCty(String cty) {
		this.cty = cty;
	}

	public String getLon() {
		return lon;
	}

	public void setLon(String lon) {
		this.lon = lon;
	}

	public String getLan() {
		return lan;
	}

	public void setLan(String lan) {
		this.lan = lan;
	}

	public String getDma() {
		return dma;
	}

	public void setDma(String dma) {
		this.dma = dma;
	}

	public String getCtry() {
		return ctry;
	}

	public void setCtry(String ctry) {
		this.ctry = ctry;
	}

}
