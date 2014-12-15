package com.sharethis.delivery.estimation;

import java.util.List;
import java.util.Map;

public abstract class Distribution {
	protected Map<Integer, List<Double>> i;
	protected Map<Integer, List<Double>> c;
	protected double[] ao, io;
	protected boolean isConstraintPerDegree;
	protected static int K;
	protected int degrees, constraints;
	private final static double lambda = 2.0;
	private final static double minIo = 1000.0;
	
	protected Distribution(Map<Integer, List<Double>> i, Map<Integer, List<Double>> c, double[] ao, double[] wo) {
		this.i = i;
		this.c = c;
		this.ao = ao;
		K = c.get(0).size();
		degrees = ao.length;
		constraints = c.size();
		isConstraintPerDegree = (constraints == degrees);
		
		int k0 = (isConstraintPerDegree ? 0 : Math.max(K - 14, 0));
		double priorWeight = weight(k0 - 1);
		
		io = new double[degrees];
		for (int p = 0; p < degrees; p++) {
			io[p] = priorWeight * wo[p];
			List<Double> ip = i.get(p);
			List<Double> cp = c.get(isConstraintPerDegree ? p : 0);
			for (int k = k0; k < K; k++) {
				if (cp.get(k) == null) continue;
				io[p] += ip.get(k) * weight(k);
			}
			io[p] = (p == 0 ? 0.1 : 0.5) * io[p];
			io[p] = Math.max(io[p], minIo);
		}
	}
	
	protected static double weight(int k) {
		double x = (double) (K - 1 - k) / lambda;
		return Math.exp(-x);
		// return Math.pow(1.0 / (x + 1.0), 1.0);
	}
	
	public static double average(List<Double> h) {
		double ave = 0.0;
		double norm = 0.0;
		K = h.size();
		for (int k = 0; k < K; k++) {
			ave += h.get(k) * weight(k);
			norm += weight(k);
		}
		return (ave/norm);
	}
}
