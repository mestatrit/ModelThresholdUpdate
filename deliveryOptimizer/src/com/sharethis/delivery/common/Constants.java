package com.sharethis.delivery.common;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Constants {
	public static final String LOGGER_NAME = "DO";

	public static final String DEFAULT = "default";

	public static final String LOG4J_PROPERTIES = "log4jp";
	public static final String DELIVERY_OPTIMIZER_PROPERTIES = "dop";
	public static final String INPUT_DATA_PROPERTY = "data";
	public static final String INPUT_PRICE_VOLUME_PROPERTY = "pv";
	public static final String INPUT_PRICE_VOLUME_DATE_PROPERTY = "date";
	public static final String INPUT_CASE_PROPERTY = "case";
	public static final String OUTPUT_DELIVER_ADX_PROPERTY = "adx";
	public static final String OUTPUT_DELIVER_RTB_PROPERTY = "rtb";
	public static final String REPORT_TYPE_PROPERTY = "type";
	public static final String REPORT_USER_PROPERTY = "user";
	public static final Set<String> INPUT_DATA_SOURCES = new HashSet<String>(Arrays.asList("adx", "rtb"));
	public static final Set<String> INPUT_TYPES = new HashSet<String>(Arrays.asList("imp", "kpi", "all"));

	public static final long MILLISECONDS_PER_HOUR = 60L * 60L * 1000L;
	public static final int DAYS_UPDATE_AFTER_END_DATE = 2;
	public static final int MAX_IMPRESSION_TARGET = 1000000;
	public static final double MAX_DAILY_SPEND = 10000.0;
	public static final boolean UPDATE_IMPRESSION_TARGET = false;
	public static final String END_OF_CAMPAIGN_DELIVERY_PERIOD_DAYS = "end_of_campaign_delivery_period_days";
	public static final String SEGMENT_ONE_FACTOR = "segment_one_factor";
	public static final double PV_MIN_R2 = 0.75;
	public static final double PV_CPM_IMPRESSION_FACTOR = 1.2;
	public static final int PV_MIN_VALID_TOTAL_INVENTORY = 1000;
	public static final double PV_MOVING_AVERAGE_SMOOTHING_FACTOR = 0.5;

	public static final int WEEKDAYS = 7;
	public static final double[] NONUNIFORM_DELIVERY_FACTOR = { 0.33, 1.0, 1.44, 1.46, 1.44, 1.0, 0.33 };
	public static final double[] WEEKDAY_DELIVERY_FACTOR = { 0.0, 1.2, 1.53, 1.54, 1.53, 1.2, 0.0 };
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
	
	public static final String CAMPAIGN_ID = "campaignId";
	public static final String CAMPAIGN_NAME = "campaignName";
	public static final String GOAL_ID = "goalId";
	public static final String START_DATE = "startDate";
	public static final String END_DATE = "endDate";
	public static final String STATUS = "status";
	public static final String BUDGET = "budget";
	public static final String MIN_MARGIN = "minMargin";
	public static final String CPA = "cpa";
	public static final String MAX_ECPA = "maxEcpa";
	public static final String CPM = "cpm";
	public static final String SEGMENTS = "segments";
	public static final String PRECEDING_CAMPAIGN_IDS = "precedingCampaignIds";
	public static final String CONVERSION_RATIO = "conversionRatio";
	public static final String PRIOR_WEIGHT = "priorWeight";
	public static final String DELIVERY_INTERVAL = "deliveryInterval";
	public static final String LEAVE_OUT_IMPRESSIONS = "leaveOutImpressions";
	public static final String MODULATE = "modulate";
	public static final String STRATEGY = "strategy";
	public static final String END_OF_CAMPAIGN_DELIVERY_MARGIN = "endOfCampaignDeliveryMargin";
	public static final String CAMPAIGN_DELIVERY_MARGIN = "campaignDeliveryMargin";
	public static final String CONVERSIONS_PER_INTERVAL = "conversionsPerInterval";
	public static final String IMPRESSIONS_PER_INTERVAL = "impressionsPerInterval";
	public static final String REVERSION_INTERVALS = "reversionIntervals";

	public static String DB_USER = "db_user";
	public static String DB_PASSWORD = "db_password";

	public static String ADX_CLIENT_ID = "adx_client_id";
	public static String ADX_DEVELOPER_TOKEN = "adx_developer_token";
	public static String ADX_EMAIL = "adx_email";
	public static String ADX_DECRYPTION_KEY = "adx_decryption_key";
	public static String ADX_ENCRYPTION_KEY = "adx_encryption_key";
	public static String ADX_INTEGRITY_KEY = "adx_integrity_key";
	public static String ADX_PASSWORD = "adx_password";
	public static String ADX_USER_AGENT = "adx_user_agent";
	public static String ADX_USE_SANDBOX = "adx_use_sandbox";

	public static String URL_DELIVERY_OPTIMIZER = "url_delivery_optimizer";
	public static final String DO_AD_GROUP_MAPPINGS = "adGroupMappings";
	public static final String DO_AD_GROUP_SETTINGS = "adGroupSettings";
	public static final String DO_CAMPAIGN_SETTINGS = "campaignSettings";
	public static final String DO_THIRD_PARTY_KPI_MAPPINGS = "thirdPartyKpiMappings";
	public static final String DO_CAMPAIGN_PERFORMANCES = "campaignPerformances";
	public static final String DO_CAMPAIGN_PERFORMANCES_BACKUP = "campaignPerformances_backup";
	public static final String DO_IMPRESSION_TARGETS = "impressionTargets";
	public static String DO_PRICE_VOLUME_CURVES = "priceVolumeCurves";
	public static final String DO_PRICE_VOLUME_CURVES_BACKUP = "priceVolumeCurves_backup";
	public static final String DO_GLOBAL_PARAMETERS = "globalParameters";
	public static final String DO_ADMIN_DATE = "admin_date";

	public static String URL_RTB = "url_rtb";
	public static final String RTB_AD_GROUP_DELIVERY = "RTB_AdGroupDelivery";
	public static final String RTB_AD_GROUP_PROPERTY = "RTB_AdGroupProperty";

	public static String URL_CAMPAIGN = "url_campaign";
	public static final String CAMPAIGN_DB_AD_GROUP = "adgroup";

	public static String URL_RTB_STATISTICS = "url_rtb_statistics";
	public static final String RTB_STATISTICS_DB_DELIVERY_REPORT = "deliveryReport";
	public static final long ATF_PLACEMENT = 1L << 12;	//4096;
	
	public static String ADX_FILE = "adx_file";
	public static final String TEMPORARY_REPORT = "deliveryOptimizer.audiencesReport.tsv.tmp";

	public static final String RUN_UPDATE = "run_update";
	public static final String RUN_TRANSFER = "run_transfer";
	public static final String RUN_OPTIMIZE = "run_optimize";
	public static final String RUN_DELIVER = "run_deliver";
	
	public static String DO_TRANSFER_MISSING_KPI_DATES_FILE = "transfer_missing_kpi_dates_file";

	public static String URL_JUNIT = "url_junit";
}
