package com.sharethis.adquality.IASReport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.junit.Test;

public class AuthenticatedHttpURLConnectionTest {

	@Test
	public void test2() {
		String url="https://integralplatform.com/reportingservice/j_spring_security_check";
				//"https://integralplatform.com/reportingservice/j_spring_security_check";
		String username = "sharethis";
		String password = "integral_2013";
		AuthenticatedHttpURLConnection authConn = new AuthenticatedHttpURLConnection(url,username,password,5);
		authConn.authenticate();
		System.out.println("Auth cookie"+authConn.auth_cookie);
		//Sample from api doc
		//https://integralplatform.com/reportingservice/api/teams/251/fw/campaignstatus/active/viewability3ms?period=yesterday&groups=[camp]
		String reportUrl = "https://integralplatform.com/reportingservice/api/teams/1314/cm/campaigns/9728564940/campaignviewability?&period=last7days&cutoff=1000&groups=[camp]&includeBenchmark=true&_search=false&rows=5000&page=1&sortIndex=totalImpressions&sortOrder=desc&totalrows=5000";
		//"https://integralplatform.com/reportingservice/api/teams/19748/fw/campaigns/9728564940/viewability3ms?period=last7days";
		//"https://integralplatform.com/reportingservice/api/teams/19748/fw/9728564940/viewability3ms?period=yesterday";
		authConn.auth_cookie = "JSESSIONID=1E325913E44AD70CC073B3822A4D90F5";
		for(int i=0;i<110;i++){
			System.out.println("Try "+i);
			String report = authConn.getUrl(reportUrl);
			System.out.println(reportUrl+":"+report);
			try { Thread.sleep(1000*60*10); } catch (InterruptedException e) { e.printStackTrace(); }
		}
	}
	
	//@Test
	public void test() {
		String url="https://integralplatform.com/reportingservice/j_spring_security_check";
				//"https://integralplatform.com/reportingservice/j_spring_security_check";
		String username = "sharethis";
		String password = "integral_2013";
		AuthenticatedHttpURLConnection1 authConn = new AuthenticatedHttpURLConnection1(url,username,password);
		authConn.authenticate();
		System.out.println(authConn.auth_cookie);
		//Sample from api doc
		//https://integralplatform.com/reportingservice/api/teams/251/fw/campaignstatus/active/viewability3ms?period=yesterday&groups=[camp]
		String reportUrl = "https://integralplatform.com/reportingservice/api/teams/1314/cm/campaigns/9728564940/campaignviewability?&period=last7days&cutoff=1000&groups=[camp]&includeBenchmark=true&_search=false&rows=5000&page=1&sortIndex=totalImpressions&sortOrder=desc&totalrows=5000";
		//"https://integralplatform.com/reportingservice/api/teams/19748/fw/campaigns/9728564940/viewability3ms?period=last7days";
		//"https://integralplatform.com/reportingservice/api/teams/19748/fw/9728564940/viewability3ms?period=yesterday";
		//authConn.auth_cookie = "JSESSIONID=0F7BD83D15AD58C460F443C8F8513E28";
		for(int i=0;i<11;i++){
			System.out.println("Try "+i);
			String report = authConn.getUrl(reportUrl);
			System.out.println(reportUrl+":"+report);
		}
	}
	
	public void sslServerTest() throws NoSuchAlgorithmException, KeyManagementException, IOException{
		// Create a trust manager that does not validate certificate chains
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

		URL url = new URL("https://www.google.com");
		URLConnection con = url.openConnection();
		final Reader reader = new InputStreamReader(con.getInputStream());
		final BufferedReader br = new BufferedReader(reader);        
		String line = "";
		while ((line = br.readLine()) != null) {
			System.out.println(line);
		}        
		br.close();
	} // End of main 

}
