package com.sharethis.delivery.base;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class StrategyType {
	private static final Logger log = Logger.getLogger(Constants.DO_LOGGER_NAME);
	private static final String sep = ":";
	private static final String number = "((-|\\+)?[0-9]+(\\.[0-9]+)?)+";
	private static Set<Long> holidays;
	
	private enum Method {
		OPTIMIZED, MAXIMIZED, RATIO, FIXED;
	}
	private enum Pacing {
		UNIFORM, NONUNIFORM, WEEKDAY, PAUSED;
	}
	
	private Method method;
	private Pacing pacing;
	private String strategy;

	private double ratio;
	private int rtImpressions, nrtImpressions;
	private static Map<Pacing, double[]> deliveryFactor;
	private double holidayDeliveryFactor;
	
	public StrategyType() {
		method = Method.OPTIMIZED;
		pacing = Pacing.PAUSED;
		strategy = "paused";
		rtImpressions = 0;
		nrtImpressions = 0;
	}
	
	public StrategyType(Method method, Pacing pacing) {
		initialize();
		this.method = method;
		this.pacing = pacing;
	}
	
	public StrategyType(int nrtImpressions) {
		initialize();
		this.method = Method.MAXIMIZED;
		this.pacing = Pacing.UNIFORM;
		this.nrtImpressions = nrtImpressions;
	}
	
	public StrategyType(int rtImpressions, Pacing pacing) {
		initialize();
		this.method = Method.FIXED;
		this.pacing = pacing;
		this.rtImpressions = rtImpressions;
	}
	
	public StrategyType(int rtImpressions, int nrtImpressions) {
		initialize();
		this.method = Method.FIXED;
		this.pacing = Pacing.UNIFORM;
		this.rtImpressions = rtImpressions;
		this.nrtImpressions = nrtImpressions;
	}
	
	public StrategyType(Pacing pacing, double ratio) {
		initialize();
		this.method = Method.RATIO;
		this.pacing = pacing;
		this.ratio = ratio;
	}
	
	public static StrategyType unknown(String strategy) {
		StrategyType unknown = new StrategyType(null, null);
		unknown.strategy = strategy;
		return unknown;
	}
	
	private void initialize() {
		ratio = 1.0;
		rtImpressions = Integer.MAX_VALUE;
		nrtImpressions = Integer.MAX_VALUE;
		deliveryFactor = new HashMap<Pacing, double[]>();
		deliveryFactor.put(Pacing.UNIFORM, Constants.UNIFORM_DELIVERY_FACTOR);
		deliveryFactor.put(Pacing.NONUNIFORM, Constants.NONUNIFORM_DELIVERY_FACTOR);
		deliveryFactor.put(Pacing.WEEKDAY, Constants.WEEKDAY_DELIVERY_FACTOR);
	}
	
	public static void setHolidays() throws SQLException, ParseException {
		holidays = new LinkedHashSet<Long>();
		String today = DateUtils.getDate(-31);
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String.format("SELECT date FROM holidays WHERE campaignId = 0 AND date >= '%s' ORDER BY date", today);
		ResultSet result = statement.executeQuery(query);
		while (result.next()) {
			Date date = result.getDate("date");
			holidays.add(date.getTime());
		}
		if (holidays.size() > 0) {
			StringBuilder builder = new StringBuilder();
			builder.append("Holidays:");
			for (long time : holidays) {
				builder.append(" " + DateUtils.getDate(time));
			}
			log.info(builder.toString());
		}
	}
	
	private static boolean isValidSyntax(String strategy) {
		strategy = strategy.replaceAll("\\s*:\\s*", ":").toUpperCase().trim();
		String[] values = strategy.split("\\s+");
		if (values.length > 2) {
			return false;
		} else if (values.length == 1) {
			return isValidPacing(values[0]);
		} else {
			String[] segment1 = values[0].trim().split(sep);
			String[] segment2 = values[1].trim().split(sep);
			return isValidPacing(segment1, segment2);
		}
	}
	
	private static boolean isValidPacing(String[] str1, String[] str2) {
		int dim = str1.length + str2.length;
		if (!isValidPacing(str1)) {
			return false;
		} else if (!isValidPacing(str2)) {
			return false;
		} else if (dim == 3) {
			return (str1[0].equals("RATIO") || str2[0].equals("RATIO"));
		} else if (dim == 4) {
			boolean hasValidSegments = str1[0].equals("RT") && str2[0].equals("NRT") || str1[0].equals("NRT") && str2[0].equals("RT");
			if (!hasValidSegments) {
				return false;
			}
			String rt  = (str1[0].equals("RT")  ? str1[1] : str2[1]);
			String nrt = (str1[0].equals("NRT") ? str1[1] : str2[1]);
			return ((rt.equals("MAX") || rt.matches(number)) && (isValidPacing(nrt) || nrt.matches(number)));
		} else {
			return false;
		}
	}
	
	private static boolean isValidPacing(String[] str) {
		if (str.length > 2) {
			return false;
		} else if (str.length == 1) {
			return isValidPacing(str[0]);
		} else if (str[0].equals("RT")) {
			return (str[1].equals("MAX") || str[1].matches(number));
		} else if (str[0].equals("NRT")) {
			return isValidPacing(str[1]) || str[1].matches(number);
		} else {
			return (str[0].equals("RATIO") && str[1].matches(number));
		}
	}

	private static boolean isValidPacing(String str) {
		for (Pacing pacing : Pacing.values()) {
			if (pacing.toString().equals(str.trim())) {
				return true;
			}
		}
		return false;
	}
	
	public static StrategyType parseStrategy(String strategy) {
		if (strategy == null || strategy.trim().isEmpty()) {
			log.error(String.format("Missing strategy type"));
			return StrategyType.unknown("");
		}
		if (!isValidSyntax(strategy)) {
			log.error(String.format("Unknown or invalid strategy type: %s", strategy));
			return StrategyType.unknown(strategy);
		}
		strategy = strategy.replaceAll("\\s*:\\s*", ":").toUpperCase().trim();
		String[] values = strategy.split("\\s+");
		if (values.length == 1) {
			for (Pacing pacing : Pacing.values()) {
				if (pacing.toString().equals(values[0].trim())) {
					return new StrategyType(Method.OPTIMIZED, pacing);
				}
			}
		} else {
			String[] segment1 = values[0].trim().split(sep);
			String[] segment2 = values[1].trim().split(sep);
			int dim = segment1.length + segment2.length;
			if (dim == 3) {
				double ratio = Double.parseDouble(segment1[0].equals("RATIO") ? segment1[1] : segment2[1]);
				String strStrategy = (segment1[0].equals("RATIO") ? segment2[0] : segment1[0]);
				for (Pacing pacing : Pacing.values()) {
					if (pacing.toString().equalsIgnoreCase(strStrategy)) {
						return new StrategyType(pacing, ratio);
					}
				}
			} else {
				String rt  = (segment1[0].equals("RT")  ? segment1[1] : segment2[1]);
				String nrt = (segment1[0].equals("NRT") ? segment1[1] : segment2[1]);
				if (rt.equals("MAX")) {
					if (nrt.matches(number)) {
						return new StrategyType(Integer.parseInt(nrt));
					} else {
						for (Pacing pacing : Pacing.values()) {
							if (pacing.toString().equals(nrt)) {
								return new StrategyType(Method.MAXIMIZED, pacing);
							}
						}
					}
				} else if (rt.matches(number)) {
					int rtImpressions = Integer.parseInt(rt);
					if (nrt.matches(number)) {
						return new StrategyType(rtImpressions, Integer.parseInt(nrt));
					} else {
						for (Pacing pacing : Pacing.values()) {
							if (pacing.toString().equals(nrt)) {
								return new StrategyType(rtImpressions, pacing);
							}
						}
					}
				}
			}
		}
		log.error("Strategy parsing failed: strategy set to null (" + strategy + ")");
		return null;
	}

	public void setHolidayDeliveryFactor(double holidayDeliveryFactor) {
		this.holidayDeliveryFactor = holidayDeliveryFactor;
	}
	
	public void setEndOfCampaignDeliveryFactor() {
		deliveryFactor.put(pacing, Constants.END_OF_CAMPAIGN_DELIVERY_FACTOR);
	}

	public void setNonuniformDeliveryFactor(double[] nonuniformDeliveryFactor) {
		deliveryFactor.put(Pacing.NONUNIFORM, nonuniformDeliveryFactor);
	}

	public double getDeliveryFactor(long time, int addDays) {
		if (pacing == Pacing.PAUSED) {
			return 0.0;
		} else {
			int day = DateUtils.getIntDayOfWeek(time, addDays);
			long newTime = DateUtils.getTime(time, addDays);
			return (holidays.contains(newTime) ? holidayDeliveryFactor : 1.0)
					* (deliveryFactor.containsKey(pacing) ? deliveryFactor.get(pacing)[day] : 1.0);
		}
	}

	public String getType() {
		return strategy;
	}
	
	public double getRatio() {
		return ratio;
	}
	
	public int getRtImpressions() {
		return rtImpressions;
	}
	
	public int getNrtImpressions() {
		return nrtImpressions;
	}

	public boolean isMaxRt() {
		return method == Method.MAXIMIZED;
	}

	public boolean isRatio() {
		return method == Method.RATIO;
	}
	
	public boolean isFixedRt() {
		return method == Method.FIXED;
	}
	
	public boolean isPausedDelivery() {
		return pacing == Pacing.PAUSED;
	}

	public boolean isUniformDelivery() {
		return pacing == Pacing.UNIFORM;
	}

	public boolean isWeekdayDelivery() {
		return pacing == Pacing.WEEKDAY;
	}
	
	public boolean isValid() {
		return (method != null && pacing != null);
	}
	
	public String deliveryFactorToString() {
		double[] factor = (deliveryFactor.containsKey(pacing) ? deliveryFactor.get(pacing) : new double[Constants.WEEKDAYS]);
		String str = String.format("%4.2f", factor[0]);
		for (int i = 1; i < Constants.WEEKDAYS; i++) {
			str += String.format(" %4.2f", factor[i]);
		}
		return str;
	}
}
