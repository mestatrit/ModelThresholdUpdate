package com.sharethis.adquality.IASReport;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.Consts;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.sharethis.common.helper.log.LogHelper;
import com.sharethis.iasapi.IasCampaignViewabiltyReport;

public class AuthenticatedHttpURLConnection {
	private static Logger log = Logger.getLogger(AuthenticatedHttpURLConnection.class);
	String auth_url;
	String username;
	String password;
	String auth_cookie;
	boolean isAuthenticated =false;
	CloseableHttpClient httpclient;
	CookieStore cookieStore = new BasicCookieStore();
	
	AuthenticatedHttpURLConnection(String url, String username, String password,int timeout){
		this.auth_url = url;
		this.username = username;
		this.password = password;
		LaxRedirectStrategy redirectStrategy = new LaxRedirectStrategy();
		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(timeout * 1000).build();
		httpclient = HttpClients.custom()
		        .setRedirectStrategy(redirectStrategy)
		        .setDefaultCookieStore(cookieStore)
		        .setDefaultRequestConfig(requestConfig)
		        .build();
	}
	
	public int authenticate() {
		int ret =0;
		HttpClientContext context = HttpClientContext.create();
		
	
			CloseableHttpResponse response=null;
		try {	
			List<NameValuePair> formparams = new ArrayList<NameValuePair>();
			formparams.add(new BasicNameValuePair("j_username", username));
			formparams.add(new BasicNameValuePair("j_password", password));
			UrlEncodedFormEntity entity = new UrlEncodedFormEntity(formparams, Consts.UTF_8);
			HttpPost httppost = new HttpPost(auth_url);
			httppost.setEntity(entity);
			response = httpclient.execute(httppost,context);
			
			System.out.println("Response status="+response.getStatusLine().toString());
			
			HttpHost target = context.getTargetHost();
		    List<URI> redirectLocations = context.getRedirectLocations();
		    URI location = URIUtils.resolve(httppost.getURI(), target, redirectLocations);
		    System.out.println("Final HTTP location: " + location.toASCIIString());
		    System.out.println("Start cookieStore");
		    for ( Cookie c: cookieStore.getCookies()){
		    	System.out.println(c.getName()+"="+c.getValue());
		    	if(c.getName().compareToIgnoreCase("JSESSIONID")==0){
		    		this.auth_cookie = c.getValue();
		    	}
		    }
		    System.out.println("End cookieStore");
			HeaderElementIterator it = new BasicHeaderElementIterator(
				    response.headerIterator("Set-Cookie"));
			System.out.println("Start cookie");
				while (it.hasNext()) {
				    HeaderElement elem = it.nextElement(); 
				    System.out.println(elem.getName() + " = " + elem.getValue());
				    NameValuePair[] params = elem.getParameters();
				    for (int i = 0; i < params.length; i++) {
				        System.out.println(" " + params[i]);
				    }
				}
				System.out.println("End cookie");
			
		} catch (ClientProtocolException e) {
			ret=-1;
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			ret=-1;
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if(response!=null)
					response.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return ret;
	}

	public String getUrl(String reportUrl) {
		//CloseableHttpClient httpclient = HttpClients.custom()
		 //       .setDefaultCookieStore(cookieStore)
		  //      .build();
		long startTime= System.currentTimeMillis();
		HttpGet httpget = new HttpGet(reportUrl);
		CloseableHttpResponse response=null;
		String jsonString ="";
		try {
			response = httpclient.execute(httpget);
			HttpEntity entity = response.getEntity();
			if (entity != null) {
		        long len = entity.getContentLength();
		        if (len != -1 && len < 2048) {
		            System.out.println("Entity:"+EntityUtils.toString(entity));
		        } else {
		            // Stream content out
		        	InputStream is = entity.getContent();
		        	jsonString = IOUtils.toString(is, "UTF-8");
		        	
		        }
		    }
		} catch (ClientProtocolException e) {
			log.error(LogHelper.formatMessage(e));
		} catch (IOException e) {
			log.error(LogHelper.formatMessage(e));
		}finally{

			try {
				if(response!=null)
					response.close();
			} catch (IOException e) {
				log.error(LogHelper.formatMessage(e));
			}
		}
		log.error("duration="+ (System.currentTimeMillis()-startTime));
		return jsonString;
	}	
}
