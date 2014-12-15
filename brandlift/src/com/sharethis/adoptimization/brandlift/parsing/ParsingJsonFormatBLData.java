package com.sharethis.adoptimization.brandlift.parsing;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.google.gson.Gson;

public class ParsingJsonFormatBLData extends BaseOperation implements Function 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;	
	private String[] retargJsonNames = null;
	private String[] blNames = null;

	public ParsingJsonFormatBLData(Fields fieldDeclaration, String[] retargJsonNames, String[] blNames)
    {
		super(fieldDeclaration);
		this.retargJsonNames = retargJsonNames;
		this.blNames = blNames;
    }

	public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
		TupleEntry arguments = functionCall.getArguments();
		String jsonRow = arguments.getString(0);
		Gson gson = new Gson();
		if(jsonRow!=null && !("null".equalsIgnoreCase(jsonRow.toString()))&&!((jsonRow.toString()).isEmpty())){
			BrandLiftEventDataObject obj = new BrandLiftEventDataObject();
			obj = gson.fromJson(jsonRow, BrandLiftEventDataObject.class);
			Map dMap = obj.toMap();
			Tuple group = new Tuple();
			for(int i=0; i<retargJsonNames.length; i++){
				Tuple gp = new Tuple(dMap.get(retargJsonNames[i]));
				group.addAll(gp);
			}

			String urlStr = (String) dMap.get("url");
			if(urlStr!=null&&!"null".equalsIgnoreCase(urlStr)&&!urlStr.isEmpty()){
				Map<String, String> paramMap = getUrlParameters(urlStr);
				if(paramMap!=null&&!paramMap.isEmpty()&&paramMap.size()!=0){
					for(int i_bl=0; i_bl<blNames.length; i_bl++){
						Tuple gp = new Tuple("null");
						String blValue = paramMap.get(blNames[i_bl]);
						if(!"null".equalsIgnoreCase(blValue)&&blValue!=null&&!blValue.isEmpty()){
							gp = new Tuple(blValue);
						}
						group.addAll(gp);						
					}
				}else{
					for(int i_bl=0; i_bl<blNames.length; i_bl++){
						Tuple gp = new Tuple("null");
						group.addAll(gp);
					}									
				}
			}else{
				for(int i_bl=0; i_bl<blNames.length; i_bl++){
					Tuple gp = new Tuple("null");
					group.addAll(gp);
				}				
			}
			functionCall.getOutputCollector().add(group);		
		}else{
			System.out.println("The row is empty!");
		}
    }
	
	public static final String _outputDelimiter = "\t";
	private static final String EqualSymbol = "=";
	private static final String Ampersand = "&";
			
	public Map<String, String> getUrlParameters(String paramStr)
    {
		Map<String, String> paramMap = new HashMap<String, String>();
		if (paramStr == null || paramStr.isEmpty() 
				|| "null".equalsIgnoreCase(paramStr)) return paramMap;
		
		// remove everything before ? mark
		String token2 = paramStr.substring(paramStr.indexOf("?") + 1);
		if (token2 == null || token2.isEmpty()) return paramMap;
		
		String[] paramPairs = StringUtils.split(token2, Ampersand);
		for(int i=0; i<paramPairs.length; i++){
			if(paramPairs[i]!=null&&!"null".equalsIgnoreCase(paramPairs[i])&&paramPairs[i].contains(EqualSymbol)){
				String[] paramValues = StringUtils.split(paramPairs[i], EqualSymbol);
				if(paramValues!=null&&(paramValues.length==2)){
					paramMap.put(paramValues[0], paramValues[1]);
					if(paramValues[0].equalsIgnoreCase("vzc6"))
						paramMap.put("retarg_jid", paramValues[1]);
				}
			}
		}
		
		return paramMap;
	}	
}
