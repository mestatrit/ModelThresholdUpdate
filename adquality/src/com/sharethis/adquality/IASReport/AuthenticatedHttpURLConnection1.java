package com.sharethis.adquality.IASReport;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.CookieStore;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.log4j.Logger;

import com.sharethis.common.error.ErrorCode;
import com.sharethis.common.helper.log.LogHelper;

public class AuthenticatedHttpURLConnection1 {
	private static Logger log = Logger.getLogger(AuthenticatedHttpURLConnection1.class);
	String auth_url;
	String username;
	String password;
	String auth_cookie;
	boolean isAuthenticated =false;
	
	AuthenticatedHttpURLConnection1(String url, String username, String password){
		this.auth_url = url;
		this.username = username;
		this.password = password;
	}
	
	public int authenticate() {
		HttpURLConnection conn=sendAuthenticationRequest();
		int ret=ErrorCode.ERROR_SUCCESS;
		if(conn!=null){
			if(getAuthenticationResponse(conn)!=ErrorCode.ERROR_SUCCESS){
				
			}
			conn.disconnect();
		}else
			ret = ErrorCode.ERROR_GENERAL;
		return ret;
	}
	public String getUrl(String url){
		HttpURLConnection conn=sendRequest(url);
		String resp=null;
		if(conn!=null){
			resp= getResponse(conn);
			conn.disconnect();
		}
		return resp;
		
	}
	private HttpURLConnection sendRequest(String urlString){
		URL url;
		HttpURLConnection conn = null ;
		try {
		url = new URL(urlString);
		//send request
		//HttpURLConnection.setFollowRedirects(true);
		conn = (HttpURLConnection) url.openConnection();
		conn.setRequestProperty("Cookie",this.auth_cookie);
		System.out.println("Cookie:"+this.auth_cookie);
		//conn.setReadTimeout(10000);
		conn.setConnectTimeout(15000);
		//conn.setRequestMethod("GET");
		//conn.setDoInput(true);
		
		conn.connect();
		
		
		} catch (IOException e) {
			log.error(LogHelper.formatMessage(e));
		}
		return conn;
	}
	private String getResponse(HttpURLConnection conn) {
		int ret=ErrorCode.ERROR_SUCCESS;
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		byte[] buf = new byte[4096];
		String resp=null;
        try {
        	InputStream is = conn.getInputStream();
        	int res=0;
			while ((res = is.read(buf)) > 0) {
			    os.write(buf, 0, res);
			}
			is.close();
			resp = new String(os.toByteArray());
		} catch (IOException e) {
			ret=ErrorCode.ERROR_INVALID_DATA;
			//e.printStackTrace();
			log.error("IOException:"+e.toString());
			try {
	            int respCode = conn.getResponseCode();
	            InputStream es = conn.getErrorStream();
	            ret = 0;
	            // read the response body
	            while ((ret = es.read(buf)) > 0) {
	                os.write(buf, 0, ret);
	            }
	            // close the errorstream
	            es.close();
	            log.error( "Error response " + respCode + ": " + 
	               new String(os.toByteArray()));
	        } catch(IOException ex) {
	            //throw ex;
	        	//ex.printStackTrace();
	        	log.error("IOException^2:"+LogHelper.formatMessage(e));
	        }
			
		}
		return resp;
	}
	private int getAuthenticationResponse(HttpURLConnection conn) {
		int ret=ErrorCode.ERROR_SUCCESS;
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		byte[] buf = new byte[4096];
        try {
        	InputStream is = conn.getInputStream();
        	int res=0;
			while ((res = is.read(buf)) > 0) {
			    os.write(buf, 0, res);
			}
			is.close();
			getCookies();
			int responseCode = conn.getResponseCode();
			System.out.println("responseCode="+responseCode);
			//this.auth_cookie = new String(os.toByteArray());
		} catch (IOException e) {
			ret=ErrorCode.ERROR_INVALID_DATA;
			//e.printStackTrace();
			log.error("IOException:"+e.toString());
			try {
	            int respCode = conn.getResponseCode();
	            InputStream es = conn.getErrorStream();
	            ret = 0;
	            // read the response body
	            while ((ret = es.read(buf)) > 0) {
	                os.write(buf, 0, ret);
	            }
	            // close the errorstream
	            es.close();
	            log.error( "Error response " + respCode + ": " + 
	               new String(os.toByteArray()));
	        } catch(IOException ex) {
	            //throw ex;
	        	//ex.printStackTrace();
	        	log.error("IOException^2:"+LogHelper.formatMessage(e));
	        }
			
		}
		return ret;
	}
	private void getCookies(){
		CookieManager cookieManager = (CookieManager)CookieHandler.getDefault();
    	List<HttpCookie> cookieList = cookieManager.getCookieStore().getCookies();
    	System.out.println("StartCookies");
    	this.auth_cookie="";
    	for(HttpCookie c: cookieList){
    		this.auth_cookie = this.auth_cookie.concat(c.toString());
    		System.out.println("auth_cookie="+this.auth_cookie);
    		System.out.println(c.toString()+"\n"+c.getName()+":"+c.getValue());
    	}
    	System.out.println("EndCookies:auth_cookie="+this.auth_cookie);
	}
	private HttpURLConnection sendAuthenticationRequest() {
		URL url;
		HttpURLConnection conn = null ;
		try {
			TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
				public java.security.cert.X509Certificate[] getAcceptedIssuers() {
					return null;
				}
				public void checkClientTrusted(X509Certificate[] certs, String authType) {
				}
				public void checkServerTrusted(X509Certificate[] certs, String authType) {
				}
			} };
			// Install the all-trusting trust manager
			final SSLContext sc = SSLContext.getInstance("SSL");
			sc.init(null, trustAllCerts, new java.security.SecureRandom());
			HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
			// Create all-trusting host name verifier
			HostnameVerifier allHostsValid = new HostnameVerifier() {
				public boolean verify(String hostname, SSLSession session) {
					return true;
				}
			};

			// Install the all-trusting host verifier
			HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
			CookieHandler.setDefault(new CookieManager( null, CookiePolicy.ACCEPT_ALL));
			url = new URL(this.auth_url);
			//send request
			conn = (HttpsURLConnection) url.openConnection();
			conn.setInstanceFollowRedirects(true);
			conn.setRequestProperty("Accept","*/*");
			conn.setRequestProperty("Connection", "Keep-Alive");
			conn.setRequestProperty("Host", "integralplatform.com");
			conn.setRequestProperty("Accept-Encoding","gzip, deflate");
			conn.setRequestProperty("Content­-Type", "application/x-­www-­form­-urlencoded");
			conn.setReadTimeout(10000);
			conn.setConnectTimeout(15000);
			conn.setRequestMethod("POST");
			conn.setDoInput(true);
			conn.setDoOutput(true);

			String postParams = "j_username="+this.username+"&j_password="+this.password;

			OutputStream os = conn.getOutputStream();
			BufferedWriter writer = new BufferedWriter(
					new OutputStreamWriter(os, "UTF-8"));
			writer.write(postParams);
			writer.flush();
			writer.close();
			os.close();
			conn.connect();
			boolean redirect = false;

			// normally, 3xx is redirect
			int status = conn.getResponseCode();
			System.out.println("status : " + status);
			if (status != HttpURLConnection.HTTP_OK) {
				if (status == HttpURLConnection.HTTP_MOVED_TEMP
						|| status == HttpURLConnection.HTTP_MOVED_PERM
					|| status == HttpURLConnection.HTTP_SEE_OTHER)
			redirect = true;
		}
		if (redirect) {
			 
			// get redirect url from "location" header field
			String newUrl = conn.getHeaderField("Location");
	 
			// get the cookie if need, for login
			String cookies = conn.getHeaderField("Set-Cookie");
	 
			// open the new connnection again
			conn = (HttpURLConnection) new URL(newUrl).openConnection();
			conn.setRequestProperty("Cookie", cookies);
			conn.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
			conn.addRequestProperty("User-Agent", "Mozilla");
			//conn.addRequestProperty("Referer", "google.com");
	 
			System.out.println("cookies : " + cookies);
			System.out.println("Redirect to URL : " + newUrl);
	 
		}
		} catch (IOException e) {
			log.error(LogHelper.formatMessage(e));
		}catch (NoSuchAlgorithmException e) {
			log.error(LogHelper.formatMessage(e));
		} catch (KeyManagementException e) {
			// TODO Auto-generated catch block
			log.error(LogHelper.formatMessage(e));
		}
		return conn;
	}

	private SSLSocketFactory createSSLFactor() {
		// TODO Auto-generated method stub
		return null;
	}
}
