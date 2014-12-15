package com.sharethis.adoptimization.common;

public class CTRConstants {
	
	public static final String[] hourFolders = new String[]{"00","01","02","03","04","05","06","07","08","09","10",
			"11","12","13","14","15","16","17","18","19","20","21","22","23"};
	public static final int testFlag = 1;	
	
 	public static final String[] impJsonNames = new String[]{"date","ts","cpnm","jid","cpid",
 			"agid","ctid","cth","ctw","dmn","did","ip","usragnt","cookie",
 			"googid","src","scpid","sagid","sctid","errflg","flg","gfid","devlat","devlon"};
 
 	public static final String[] impNames = new String[]{"date","timestamp","campaign_name","jid","campaign_id",
 		"adgroup_id","creative_id","cth","ctw","domain_name","did","ip_address","user_agent","cookie",
 		"gogle_id","service_type","st_camp_id","st_adg_id","st_crtv_id","errflg","flg","gfid","devlat","devlon"};
	
 	public static final String[] clkJsonNames = new String[]{"date","ts","jid",
 		"dmn","url","ip","usragnt","cookie","src","log","zip","aid","cid"};
 	
 	public static final String[] clkNames = new String[]{"click_date","click_timestamp","click_jid",
 		"click_domain_name","click_dest_url","click_ip_address","click_user_agent","click_cookie",
 		"click_service_type","log","click_zip","click_aid","click_cid"};
}
