package com.sharethis.doreport.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

@SuppressWarnings("serial")
public class ReportServlet extends com.sharethis.common.jetty.server.ServletBase {
		
	public static final String GET = "GET";
	public static final String TEST = "t";

	public ReportServlet() {
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
		String query = request.getQueryString();
		this.log.info("query:" + query);
		Map<String, String> params = null;		
		if(query != null) {
			if(GET.equalsIgnoreCase(request.getMethod())){
				params = com.sharethis.common.helper.RequestQueryHelper.getParamMap(URLDecoder.decode(query, "UTF-8"));			
			}
			else{
				params = com.sharethis.common.helper.RequestQueryHelper.getParamMap(request.getParameterMap());			
			}			
		}
		String report = "";
		String test = params.get(TEST);
		if(test == null) {
			this.log.info("getReport");
			report = com.sharethis.delivery.job.Report.getReport(params);
		}
		else {
			this.log.info("test");
            StringBuffer sb = new StringBuffer();
            for(String key : params.keySet()) {
                    sb.append(key + "=" + params.get(key));
                    sb.append("\n");
            }
            report = sb.toString();
		}
		this.log.info("Report:");
		this.log.info(report);
		
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.print(report);
		out.close();
	}
}
