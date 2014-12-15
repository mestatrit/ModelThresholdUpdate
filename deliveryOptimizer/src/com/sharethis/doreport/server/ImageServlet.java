package com.sharethis.doreport.server;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.sharethis.common.bo.ImageData;

@SuppressWarnings("serial")
public class ImageServlet extends com.sharethis.common.jetty.server.ServletBase {
		
	HashMap<String, ImageData> imageMap = new HashMap<String, ImageData>();
	
	public ImageServlet() {
		super();
		this.log = Logger.getLogger(ReportServer.LOGGER_NAME);
	}
	
	void loadImages(String[] imageList) {
		
	}
	
	public void processGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		process(request, response);		
	}
		
	public void processPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		process(request, response);
	}
		
	public void process(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String imageName = request.getRequestURI();
		this.log.info("image:" + imageName);
		if(imageName != null) {
			ImageData data = com.sharethis.common.helper.image.ImageManager.getInstance().getImage(imageName);
			writeResponse(response, data.getData());
		}		
	}
	
	void writeResponse(HttpServletResponse response, byte[] data) throws IOException {
		if(data != null) {
			response.setContentType("image/*");
			response.setContentLength(data.length); 
			OutputStream out = response.getOutputStream(); 
			out.write(data);
			com.sharethis.common.helper.io.IOStreamHelper.safeClose(out);
		}
	}		
}

