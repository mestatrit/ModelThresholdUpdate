package com.sharethis.doreport.server;

import java.io.IOException;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.sharethis.common.helper.AppConfig;

@SuppressWarnings("serial")
public class HtmlServlet extends com.sharethis.common.jetty.server.ServletBase {

	static HashMap<String, java.io.File> _htmlMap = new HashMap<String, java.io.File>();
	
	public HtmlServlet() {
		super();
		this.log = Logger.getLogger(ReportServer.LOGGER_NAME);
	}
		
	public void processGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		process(request, response);		
	}
		
	public void processPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		process(request, response);
	}
		
	public void process(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/html");
		String uri = request.getRequestURI();
		this.log.info("query:" + uri);
		if(uri != null && uri.length() > 5) {	//*.html page is expceted									
			StringBuffer sb = new StringBuffer();
			int pos = uri.lastIndexOf("/");
			String fileName = uri.substring(pos + 1);
			String path = AppConfig.getInstance().get("html_file_path") + fileName;
			java.io.File file = _htmlMap.get(path);
			if(file == null) {
				file = new java.io.File(path);
				//_htmlMap.put(path, file);
			}			
			if(file.exists()) {
				java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(file));
				String line = null;
				while((line = br.readLine()) != null) {
					sb.append(line);					
				}
			}
			response.setContentLength(sb.length());
			java.io.PrintWriter writer = response.getWriter();
			writer.write(sb.toString());
			writer.close();
		}		
	}
}
