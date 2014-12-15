package com.sharethis.adoptimization.pricevolume;

import java.net.URI;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;
import eu.bitwalker.useragentutils.Version;

import com.google.gson.Gson;
import com.sharethis.adoptimization.common.MobileConstants;
import com.sharethis.common.bo.*;


/** 
 */

public class ParsingDataFields extends BaseOperation implements Function
{
	private static final Logger sLogger = Logger.getLogger(ParsingDataFields.class);
	private static final long serialVersionUID = 1L;
	
	public ParsingDataFields(Fields fields) {
		super(1, fields);
	}

	@SuppressWarnings("deprecation")
	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
	{
		// get the argument's TupleEntry
		TupleEntry arguments = functionCall.getArguments();
		String urlStr = arguments.getString(0);
		URI uri = null;
		try{
			uri = new URI(urlStr.trim());
		}catch(Exception ee){
			sLogger.error(ee);
		}
		String domain = null;
		if(uri!=null){
			domain = uri.getHost();            
			if (domain == null || "null".equalsIgnoreCase(domain) || domain.isEmpty()) 
				domain = uri.toString();
		}
		
		String user_browser = null;
		String user_os = null;
		String user_device = null;
		String user_agent = arguments.getString(1);
		if(!"null".equalsIgnoreCase(user_agent)&&user_agent!=null&&!user_agent.isEmpty()){
			UserAgent agent = UserAgent.parseUserAgentString(user_agent);
			if(agent!=null){
				Browser browser = agent.getBrowser();
				Version version = agent.getBrowserVersion();
				OperatingSystem os = agent.getOperatingSystem();
				if(browser!=null)
					user_browser = (browser.getName()).replaceAll("\\s","");
				if(os!=null)
					user_os = (os.getName()).replaceAll("\\s","");
				if(version!=null)
					user_device = (version.getVersion()).replaceAll("\\s","");
			}
		}
		Tuple result = new Tuple();
		result.add(domain);
		result.add(user_browser);
		result.add(user_os);
		result.add(user_device);
		String crtv_height = arguments.getString(2);
		String crtv_width = arguments.getString(3);
		int hashcode = 0;
		if(crtv_height!=null&&crtv_width!=null&&!crtv_height.isEmpty()&&!crtv_width.isEmpty()){
			int ch = 0;
			int cw = 0;
			try{
				ch = Integer.parseInt(crtv_height);
				cw = Integer.parseInt(crtv_width);
			}catch(Exception nfe){
				ch = 0;
				cw = 0;
			}
			hashcode = Size.hashCode(ch, cw);
		}
		result.add(hashcode);
		String sqi = arguments.getString(4);
		int sqi_bk = -1;
		if(sqi!=null&&!sqi.isEmpty()){
			long sqi_tmp = -1;
			try{
				sqi_tmp = Long.parseLong(sqi);
			}catch(Exception nfe){
				sqi_tmp = -1;
			}
			if(sqi_tmp>=0&&sqi_tmp<Integer.MAX_VALUE)
				sqi_bk = ((int) sqi_tmp)/10;
		}
		result.add(sqi_bk);

		//parsing the ctr model data
		int model_id = -1;
		double model_score = 1;
		String modelList = arguments.getString(5);
		if(modelList!=null&&!modelList.isEmpty()&&modelList.contains("|")){
			String[] modelListPairs = StringUtils.split(modelList, "|");
			for(int i=0; i<modelListPairs.length; i++){
				if (modelListPairs[i]!=null&&!modelListPairs[i].isEmpty()){
					String[] modelIdScore = StringUtils.split(modelListPairs[i], ",");
					int model_id_tmp = -1;
					double model_score_tmp = -1;
					try{
						model_id_tmp = Integer.parseInt(modelIdScore[0]);
						model_score_tmp = Double.parseDouble(modelIdScore[1]);
					}catch(Exception nfe){
						model_id_tmp = -1;
						model_score_tmp = -1;
					}
					if (model_id_tmp>=1000 && model_id_tmp<=5000){
						model_id = model_id_tmp;
						model_score = model_score_tmp;
						break;
					}
				}
			}
		}
		
		result.add(model_id);
		result.add(model_score);				
		
		//parsing the mobile data json object
		String mobile_info = arguments.getString(6);
//		String mobile_info = null;
		String[] mobileNames=MobileConstants.mobileJsonNames;
		if(mobile_info!=null&&!mobile_info.isEmpty()&&mobile_info.contains("{")){
			//Removing the mobile info with some characters such as '?'
			if(!mobile_info.contains("?")&&!mobile_info.contains("!")
					&&!mobile_info.contains("~")){
				Gson gson = new Gson();
				MobileDataJsonObject obj = new MobileDataJsonObject();
				try{
					obj = gson.fromJson(mobile_info, MobileDataJsonObject.class);
					Map dMap = obj.toMap();
					for(int i=0; i<mobileNames.length; i++){
						String mapValue = (String) dMap.get(mobileNames[i]);
						if("deviceId".equalsIgnoreCase(mobileNames[i])&&mapValue.contains("\n")){
							result.add("null");
						}else{
							result.add(mapValue);
						}
					}
				}catch(Exception ex){
					for(int i=0; i<mobileNames.length; i++){
						result.add("null");
					}						
				}
			}else{
				for(int i=0; i<mobileNames.length; i++){
					result.add("null");
				}						
			}		
		}else{
			for(int i=0; i<mobileNames.length; i++){
				result.add("null");
			}			
		}

		functionCall.getOutputCollector().add(result);
	}
}
