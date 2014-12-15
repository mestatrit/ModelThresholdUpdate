package com.sharethis.delivery.common;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Constants {
	public static final String DO_LOGGER_NAME = "DO";
	public static final String ADX_LOGGER_NAME = "ADX";
	
	public static final Parameters globalParameters = new Parameters();

	public static final String DEFAULT = "default";

	public static final String LOG4J_PROPERTIES = "log4jp";
	public static final String DELIVERY_OPTIMIZER_PROPERTIES = "dop";
	public static final String DELIVERY_OPTIMIZER_PATH = "path";
	public static final String INPUT_DATA_PROPERTY = "data";
	public static final String INPUT_PRICE_VOLUME_PROPERTY = "pv";
	public static final String INPUT_PRICE_VOLUME_DATE_PROPERTY = "date";
	public static final String INPUT_CASE_PROPERTY = "case";
	public static final String OUTPUT_DELIVER_ADX_PROPERTY = "adx";
	public static final String OUTPUT_DELIVER_RTB_PROPERTY = "rtb";
	public static final String REPORT_TYPE_PROPERTY = "type";
	public static final String REPORT_USER_PROPERTY = "user";
	public static final String REPORT_TIME_PROPERTY = "time";
	public static final String REPORT_STARTDATE_PROPERTY = "startDate";
	public static final String REPORT_ENDDATE_PROPERTY = "endDate";
	public static final String REPORT_MINCLK_PROPERTY = "minClk";
	public static final String REPORT_MINIMP_PROPERTY = "minImp";
	public static final String REPORT_NETWORK_PROPERTY = "network";
	public static final String MIDNIGHT = "midnight";
	public static final Set<String> INPUT_DATA_SOURCES = new HashSet<String>(Arrays.asList("adx", "rtb"));
	public static final Set<String> INPUT_TYPES = new HashSet<String>(Arrays.asList("imp", "kpi", "all"));

	public static final long MILLISECONDS_PER_HOUR = 60L * 60L * 1000L;
	public static final int DAYS_UPDATE_AFTER_END_DATE = 2;
	public static final int MAX_IMPRESSION_TARGET = 1000000;
	public static final double MAX_DAILY_SPEND = 10000.0;
	public static final boolean UPDATE_IMPRESSION_TARGET = false;
	public static final String END_OF_CAMPAIGN_DELIVERY_PERIOD_DAYS = "end_of_campaign_delivery_period_days";
	public static final String SEGMENT_ONE_FACTOR = "segment_one_factor";
	public static final double PV_MIN_R2 = 0.6;
	public static final double PV_CPM_IMPRESSION_FACTOR = 1.2;
	public static final int PV_MIN_VALID_TOTAL_INVENTORY = 1000;
	public static final double PV_MOVING_AVERAGE_SMOOTHING_FACTOR = 0.5;

	public static final int WEEKDAYS = 7;
	public static final double[] UNIFORM_DELIVERY_FACTOR = { 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0 };
	public static final double[] NONUNIFORM_DELIVERY_FACTOR = { 0.33, 1.0, 1.44, 1.46, 1.44, 1.0, 0.33 };
	public static final double[] WEEKDAY_DELIVERY_FACTOR = { 0.0, 1.2, 1.53, 1.54, 1.53, 1.2, 0.0 };
	public static final double[] DAILY_CLICK_FACTOR = { 0.33, 1.0, 1.44, 1.46, 1.44, 1.0, 0.33 };
	public static final double[] DAILY_CONVERSION_FACTOR = { 0.33, 1.0, 1.44, 1.46, 1.44, 1.0, 0.33 };
	public static final double[] END_OF_CAMPAIGN_DELIVERY_FACTOR = { 0.5, 1.2, 1.2, 1.2, 1.2, 1.2, 0.5 };

	public static final String DAILY_IMPRESSION_LIMIT = "daily_impression_limit";
	public static final String DAILY_SPEND_LIMIT = "daily_spend_limit";
	public static final String IMPRESSION_INTERVALS_LEFT = "impression_intervals_left";
	public static final String NEXT_DAY_IMPRESSION_FACTOR = "next_day_impression_factor";
	public static final String MIN_NON_RT_IMPRESSION_TARGET = "min_non_rt_impression_target";
	public static final String MIN_RT_IMPRESSION_TARGET = "min_rt_impression_target";
	public static final String KPI_GOAL_MARGIN = "kpi_goal_margin";
	public static final String AD_GROUP_ATF_FACTOR = "ad_group_atf_factor";
	public static final String DISABLE_OVER_DELIVERY_CHECK = "disable_over_delivery_check";
	
	public static final String CAMPAIGN_ID = "campaignId";
	public static final String CAMPAIGN_NAME = "campaignName";
	public static final String CAMPAIGN_TYPE = "type";
	public static final String GOAL_ID = "goalId";
	public static final String START_DATE = "startDate";
	public static final String END_DATE = "endDate";
	public static final String STATUS = "status";
	public static final String BUDGET = "budget";
	public static final String MIN_MARGIN = "minMargin";
	public static final String KPI = "kpi";
	public static final String CPA = "cpa";
	public static final String CPC = "cpc";
	public static final String MAX_EKPI = "maxEkpi";
	public static final String CPM = "cpm";
	public static final String SEGMENTS = "segments";
	public static final String PRECEDING_CAMPAIGN_IDS = "precedingCampaignIds";
	public static final String PRIOR_RATIO = "priorRatio";
	public static final String PRIOR_WEIGHT = "priorWeight";
	public static final String DELIVERY_INTERVAL = "deliveryInterval";
	public static final String LEAVE_OUT_IMPRESSIONS = "leaveOutImpressions";
	public static final String MODULATE = "modulate";
	public static final String STRATEGY = "strategy";
	public static final String END_OF_CAMPAIGN_DELIVERY_MARGIN = "endOfCampaignDeliveryMargin";
	public static final String CAMPAIGN_DELIVERY_MARGIN = "campaignDeliveryMargin";
	public static final String CONVERSIONS_PER_INTERVAL = "conversionsPerInterval";
	public static final String CLICKS_PER_INTERVAL = "clicksPerInterval";
	public static final String IMPRESSIONS_PER_INTERVAL = "impressionsPerInterval";
	public static final String REVERSION_INTERVALS = "reversionIntervals";
	public static final String TARGET_CLICKS = "targetClicks";
	public static final String TARGET_CONVERSIONS = "targetConversions";
	public static final String TARGET_IMPRESSIONS = "targetImpressions";
	public static final String KPI_ATTRIBUTION_FACTOR = "kpiAttributionFactor";	
	public static final String MANAGE_ATF = "manageAtf";	
	public static final String HOLIDAY_DELIVERY_FACTOR = "holidayDeliveryFactor";	
	
	public static final String DB_USER = "db_user";
	public static final String DB_PASSWORD = "db_password";

	public static String ADX_CLIENT_ID = "adx_client_id";
	public static String ADX_DEVELOPER_TOKEN = "adx_developer_token";
	public static String ADX_EMAIL = "adx_email";
	public static String ADX_DECRYPTION_KEY = "adx_decryption_key";
	public static String ADX_ENCRYPTION_KEY = "adx_encryption_key";
	public static String ADX_INTEGRITY_KEY = "adx_integrity_key";
	public static String ADX_PASSWORD = "adx_password";
	public static String ADX_USER_AGENT = "adx_user_agent";
	public static String ADX_USE_SANDBOX = "adx_use_sandbox";

	public static final String URL_DELIVERY_OPTIMIZER = "url_delivery_optimizer";
	public static final String DO_AD_GROUP_MAPPINGS = "adGroupMappings";
	public static final String DO_AD_GROUP_SETTINGS = "adGroupSettings";
	public static final String DO_CAMPAIGN_SETTINGS = "campaignSettings";
	public static final String DO_SEGMENT_SETTINGS = "segmentSettings";
	public static final String DO_THIRD_PARTY_KPI_MAPPINGS = "thirdPartyKpiMappings";
	public static final String DO_CAMPAIGN_PERFORMANCES = "campaignPerformances";
	public static final String DO_CAMPAIGN_PERFORMANCES_BACKUP = "campaignPerformances_backup";
	public static final String DO_SEGMENT_PERFORMANCES = "segmentPerformances";
	public static final String DO_IMPRESSION_TARGETS = "impressionTargets";
	public static String DO_PRICE_VOLUME_CURVES = "priceVolumeCurves";
	public static final String DO_PRICE_VOLUME_CURVES_BACKUP = "priceVolumeCurves_backup";
	public static final String DO_GLOBAL_PARAMETERS = "globalParameters";
	public static final String DO_ADMIN_DATE = "admin_date";
	public static final String DO_ERRORS = "errors";
	public static final String DO_ERRORS_BACKUP = "errors_backup";
	
	public static final String URL_RTB = "url_rtb";
	public static final String RTB_AD_GROUP_DELIVERY = "RTB_AdGroupDelivery";
	public static final String RTB_AD_GROUP_PROPERTY = "RTB_AdGroupProperty";
	public static final String RTB_MODEL_CONFIG = "ModelConfig";
	
	public static final String URL_ADPLATFORM = "url_adplatform";
	public static final String ADPLATFORM_DB_AD_GROUP = "adgroup";
	public static final String ADPLATFORM_DB_CAMPAIGN = "campaign";
	public static final String ADPLATFORM_DB_KPI_REPORT = "cnvReportBody";
	
	public static final String URL_RTB_STATISTICS = "url_rtb_statistics";
	public static final String RTB_STATISTICS_DB_DELIVERY_REPORT = "deliveryReport";
	public static final long ATF_PLACEMENT = 1L << 12;	//4096;
	public static final long CTR_OPTIMIZATION = 1L << 10;
	public static final long CTR_LEARNER = 1L << 25;

	public static final String URL_RTB_CAMPAIGN_STATISTICS = "url_rtb_campaign_statistics";
	public static final String RTB_CAMPAIGN_STATISTICS_DB_ADGROUP_STATS = "adgroup_stats";
	
	public static final String URL_LOOKALIKE = "url_lookalike";
	public static final String LOOKALIKE_DB_AD_GROUPS = "adGroups";
			
	public static final String RUN_UPDATE = "run_update";
	public static final String RUN_TRANSFER = "run_transfer";
	public static final String RUN_OPTIMIZE = "run_optimize";
	public static final String RUN_DELIVER = "run_deliver";
	
	public static final String TEMPORARY_REPORT = "deliveryOptimizer.audiencesReport.tsv.tmp";
	public static String DO_TRANSFER_MISSING_KPI_DATES_FILE = "transfer_missing_kpi_dates_file";
	public static String DO_ERROR_FILE = "error_file";
	
	public static String URL_JUNIT = "url_junit";
}
