package com.sharethis.delivery.optimization;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.util.DateUtils;

public enum StrategyType {

	Uniform("uniform", "uniform") {
		@Override
		public boolean isUniformDelivery() {
			return true;
		}
	},

	NonUniform("nonuniform", "nonuniform"),

	RTMax_NRTUniform("max", "uniform") {
		@Override
		public boolean isMaxRt() {
			return true;
		}
		@Override
		public boolean isUniformDelivery() {
			return true;
		}
	},

	RTMax_NRTNonUniform("max", "nonuniform") {
		@Override
		public boolean isMaxRt() {
			return true;
		}
	},

	RTMax_NRTWeekday("max", "weekday") {
		@Override
		public boolean isMaxRt() {
			return true;
		}
	},
	
	Weekday("weekday", "weekday"),
	
	RTFixed_NRTUniform("fixed", "uniform"),
	
	RTFixed_NRTNonUniform("fixed", "nonuniform"),
	
	Paused("paused", "paused") {
		@Override
		public double getDeliveryFactor(long time, int addDays) {
			return 0.0;
		}
		@Override
		public boolean isPaused() {
			return true;
		}
	};

	private final String rtStrategy, nrtStrategy;
	private final static String sep = ":";
	private static Map<String, double[]> deliveryFactor;
	private double ratio;
	private int rtImpressions, nrtImpressions;

	static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);

	private StrategyType(String rtStrategy, String nrtStrategy) {
		this.reset();
		this.rtStrategy = rtStrategy.toLowerCase();
		this.nrtStrategy = nrtStrategy.toLowerCase();
	}
	
	private void reset() {
		this.ratio = 1.0;
		this.rtImpressions = Constants.MAX_IMPRESSION_TARGET;
		this.nrtImpressions = Constants.MAX_IMPRESSION_TARGET;
		if (deliveryFactor == null) {
			deliveryFactor = new HashMap<String, double[]>();
			deliveryFactor.put("nonuniform", Constants.NONUNIFORM_DELIVERY_FACTOR);
			deliveryFactor.put("weekday", Constants.WEEKDAY_DELIVERY_FACTOR);
		}
	}
	
	public static StrategyType getDefault() {
		StrategyType defaultStrategy = Paused;
		defaultStrategy.reset();
		return defaultStrategy;
	}

	public static StrategyType fromString(String strategy) {
		if (strategy != null) {
			strategy = strategy.replaceAll("\\s*:\\s*", ":").toLowerCase().trim();
			String[] values = strategy.split("\\s+");
			if (values.length == 1) {
				for (StrategyType strategyType : values()) {
					if (strategyType.rtStrategy.equalsIgnoreCase(values[0].trim())) {
						strategyType.reset();
						return strategyType;
					}
				}
			} else {
				String[] segment1 = values[0].trim().split(sep);
				String[] segment2 = values[1].trim().split(sep);
				
				if (strategy.contains("ratio")) {
					for (StrategyType strategyType : values()) {
						if (strategyType.rtStrategy.equalsIgnoreCase(segment1[0]) && strategyType.nrtStrategy.equalsIgnoreCase(segment1[0])) {
							double ratio = Double.parseDouble(segment2[1]);
							strategyType.reset();
							strategyType.setRatio(ratio);
							return strategyType;
						}
					}
				} else if (segment1.length == 2 && segment2.length == 2) {
					boolean firstIsRt = segment1[0].equalsIgnoreCase("rt");
					String rtStrategy = (firstIsRt ? segment1[1] : segment2[1]);
					String nrtStrategy = (!firstIsRt ? segment1[1] : segment2[1]);
					if (rtStrategy.matches("((-|\\+)?[0-9]+(\\.[0-9]+)?)+")) {
						for (StrategyType strategyType : values()) {
							if (strategyType.rtStrategy.equalsIgnoreCase("fixed") && strategyType.nrtStrategy.equalsIgnoreCase(nrtStrategy)) {
								strategyType.reset();
								int rtImpressions = Integer.parseInt(rtStrategy);
//								int nrtImpressions = Integer.parseInt(nrtStrategy);
								strategyType.setRtImpressionTarget(rtImpressions);
								strategyType.setRatio(1.0);
								return strategyType;
							}
						}
					} else {
						for (StrategyType strategyType : values()) {
							if (strategyType.rtStrategy.equalsIgnoreCase(rtStrategy)
									&& strategyType.nrtStrategy.equalsIgnoreCase(nrtStrategy)) {
								strategyType.reset();
								return strategyType;
							}
						}
					}
				}
			}
		}
		
		StrategyType defaultStrategy = getDefault();
		log.error(String.format("Unknown strategy type: %s ... using: %s", strategy, defaultStrategy.toString()));
		return defaultStrategy;
	}

	public void setEndOfCampaignDeliveryFactor() {
		deliveryFactor.put(nrtStrategy, Constants.END_OF_CAMPAIGN_DELIVERY_FACTOR);
	}

	public void setNonuniformDeliveryFactor(double[] nonuniformDeliveryFactor) {
		deliveryFactor.put("nonuniform", nonuniformDeliveryFactor);
	}

	public double getDeliveryFactor(long time, int addDays) {
		int day = DateUtils.getIntDayOfWeek(time, addDays);
		return (deliveryFactor.containsKey(nrtStrategy) ? deliveryFactor.get(nrtStrategy)[day] : 1.0);
	}

	public void setRatio(double ratio) {
		this.ratio = ratio;
	}

	public void setRtImpressionTarget(int rtImpressions) {
		this.rtImpressions = rtImpressions;
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
		return false;
	}

	public boolean isPaused() {
		return false;
	}

	public boolean isUniformDelivery() {
		return false;
	}

	public boolean isFixedRatio() {
		return (ratio < 0.99);
	}
	
	public boolean isFixedDelivery() {
		return (rtImpressions < Constants.MAX_IMPRESSION_TARGET);
	}

	public boolean isWeekdayDelivery() {
		return nrtStrategy.equals("weekday");
	}
	
	public String deliveryFactorToString() {
		double[] factor = (deliveryFactor.containsKey(nrtStrategy) ? deliveryFactor.get(nrtStrategy) : new double[Constants.WEEKDAYS]);
		String str = String.format("%4.2f", factor[0]);
		for (int i = 1; i < Constants.WEEKDAYS; i++) {
			str += String.format(" %4.2f", factor[i]);
		}
		return str;
	}
	
	@Override
	public String toString() {
		return String.format("RT:%s NRT:%s", rtStrategy, nrtStrategy);
	}
	
	public boolean equals(StrategyType strategy) {
		return rtStrategy.equals(strategy.rtStrategy) && nrtStrategy.equals(strategy.nrtStrategy);
	}
}
