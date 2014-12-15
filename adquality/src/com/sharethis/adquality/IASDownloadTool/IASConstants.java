package com.sharethis.adquality.IASDownloadTool;

public interface IASConstants {
	//variables
	public static final String LOG_LEVEL="log_level";
	public static final String MAX_DL_THREADS = "max_dl_threads";
	public static final String URL_DIR = "url_dir";
	public static final String URL_FILE_REGEX="url_file_regex";
	
	//values
	public static final String URL_FILE_DELIMITER = "\t";
	public static final String MAX_IAS_RETRIES = "max_ias_retries";
	public static final String URL_CONNECTION_SIZE = "url_connection_size";
	public static final String IAS_HOST = "ias_host";
	public static final String IAS_PORT = "ias_port";
	public static final String IAS_CALL_FORMAT = "ias_call_format";
	public static final String IAS_TIMEOUT = "ias_timeout";
	public static final String HTTP_KEEP_ALIVE = "http.keepAlive";
	public static final String HTTP_MAX_CONNECTIONS = "http.maxConnections";
	public static final String OUTPUT_FILE = "output_file";
	public static final String IAS_MAX_REQ_PER_PERIOD = "ias_max_req_per_period";
	public static final String IAS_PERIOD_MSEC = "ias_period_msec";
	public static final String  MAX_OUTSTANDING_CALLS="max_outstanding_calls";
	public static final String IGNORE_IAS_ACTION = "ignore_ias_action";
	//authentication
	public static final String IAS_AUTHENTICATION_URL = "ias_authentication_url";
	public static final String IAS_USERNAME = "ias_username";
	public static final String IAS_PASSWORD = "ias_password";
	public static final String IAS_TEAM_ID = "ias_team_id";
	//Report
	public static final String ADGROUP_LIST_FILE = "adgroup_list_file";
	public static final String REPORT_FILE = "report_file";
	public static final String VIEWAB_REPORT_URL = "viewab_report_url";
	public static final String BRAND_SAFETY_REPORT_URL = "brand_safety_report_url";

	

}
