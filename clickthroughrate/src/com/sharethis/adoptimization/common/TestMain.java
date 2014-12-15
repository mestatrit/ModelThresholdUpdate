package com.sharethis.adoptimization.common;

import java.util.Map;

import com.google.gson.Gson;
import com.sharethis.adoptimization.campaigndata.HourlyDataImpressionObject;

public class TestMain  
{

 	public static final String[] impNames1 = new String[]{"date","timestamp","campaign_name","jid","campaign_id",
 		"adgroup_id","creative_id","cth","ctw","domain_name","ip_address","user_agent","cookie",
 		"gogle_id","service_type","st_camp_id","st_adg_id","st_crtv_id","errflg","flg","did"};

	public static void main (String[] args) {
		String jsonRow="{\"agid\":\"7689806700\",\"ts\":\"1381942804000\",\"dmn\":\"13bc8361eb024467.anonymous.google\",\"cpid\":\"139942740\",\"date\":" +
				"\"20131016 17:00:04\",\"sctid\":\"35298\",\"cty\":\"Ashburn\",\"googid\":\"CAESELSlxWIGPFRMwU1wak2lgqM\",\"ctw\":\"728\",\"errflg\":" +
				"0,\"src\":\"ADX\",\"jid\":\"d93549a9-7d0f-4cc7-afc6-e4db629fb54a\",\"st\":\"VA\",\"ctry\":\"United States\",\"lat\":\"39.0437\",\"ctid\":" +
				"\"30471918300\",\"asid\":\"2442047824\",\"usragnt\":\"Mozilla/4.0 (compatible; Crawler; MSIE 7.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)\"," +
				"\"evnttyp\":\"impr\",\"lon\":\"-77.4875\",\"scpid\":\"2407\",\"cpnm\":\"RTB-Sonic_2013_LFL\",\"flg\":\"0\",\"dma\":\"511\",\"did\":\"0\"," +
				"\"cth\":\"90\",\"sagid\":\"4256\",\"ip\":\"54.234.121.93\",\"cookie\":\"6C3A480AE2C45E526915D38502019869\"}";
		Gson gson = new Gson();
	
		if(jsonRow!=null && !("null".equalsIgnoreCase(jsonRow))&&!(jsonRow.isEmpty())){
			HourlyDataImpressionObject obj = gson.fromJson(jsonRow, HourlyDataImpressionObject.class);
			Map dMap = obj.toMap();
			System.out.print("JSON String: " + dMap.toString());
			for(int i=0; i<impNames1.length;i++){
				String rData = (String) dMap.get(impNames1[i]);
				//Tuple gp = new Tuple(rData);
				System.out.println("i=" + i + "   data=" + rData);
				//group.addAll(gp);
			}
		}
		
	}

}
