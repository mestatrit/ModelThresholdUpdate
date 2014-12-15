package com.sharethis.adoptimization.pricevolume;

import java.util.HashMap;
import java.util.Map;

public class MobileDataJsonObject
{	
	
	private String appId;
	private int carrierId;
	private String deviceType;
	private boolean isApp;
	private boolean isInterstitialReqauest;
	private String platform;
	private int screenOrientation;
	private String deviceId;
	public String location;
	private int device_make;
	private int device_model;
	private int deviceIdType;
			
	public Map<String, Object> toMap(){
 		Map<String, Object> dMap = new HashMap<String, Object>();
 		if(appId!=null&&!appId.isEmpty())
 			dMap.put("appId",appId);
 		else
 			dMap.put("appId","");
 		dMap.put("carrierId",Long.toString(carrierId));
 		dMap.put("deviceType",deviceType);
 		dMap.put("isApp",Boolean.toString(isApp));
 		dMap.put("isInterstitialReqauest",Boolean.toString(isInterstitialReqauest));
 		if(platform!=null&&!platform.isEmpty())
 			dMap.put("platform",platform);
 		else
 			dMap.put("platform","");
 		dMap.put("screenOrientation",Integer.toString(screenOrientation));
 		if(deviceId!=null&&!deviceId.isEmpty())
 			dMap.put("deviceId",deviceId);
 		else
 			dMap.put("deviceId","");
 		if(location!=null&&!location.isEmpty())
 			dMap.put("location",location);
 		else
 			dMap.put("location","");
 		dMap.put("device_make",Integer.toString(device_make));
 		dMap.put("device_model",Integer.toString(device_model));
 		dMap.put("deviceIdType",Integer.toString(deviceIdType));
 		
 		return dMap;
 	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public long getCarrierId() {
		return carrierId;
	}

	public void setCarrierId(int carrierId) {
		this.carrierId = carrierId;
	}

	public String getDeviceType() {
		return deviceType;
	}

	public void setDeviceType(String deviceType) {
		this.deviceType = deviceType;
	}
	
	public boolean isApp() {
		return isApp;
	}

	public void setApp(boolean isApp) {
		this.isApp = isApp;
	}

	public boolean isInterstitialReqauest() {
		return isInterstitialReqauest;
	}

	public void setInterstitialReqauest(boolean isInterstitialReqauest) {
		this.isInterstitialReqauest = isInterstitialReqauest;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public int getScreenOrientation() {
		return screenOrientation;
	}

	public void setScreenOrientation(int screenOrientation) {
		this.screenOrientation = screenOrientation;
	}

	public int getDevice_make() {
		return device_make;
	}

	public void setDevice_make(int device_make) {
		this.device_make = device_make;
	}

	public int getDevice_model() {
		return device_model;
	}

	public void setDevice_model(int device_model) {
		this.device_model = device_model;
	}

	public int getDeviceIdType() {
		return deviceIdType;
	}

	public void setDeviceIdType(int deviceIdType) {
		this.deviceIdType = deviceIdType;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}
}
