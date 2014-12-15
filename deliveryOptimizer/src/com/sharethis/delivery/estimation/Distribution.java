package com.sharethis.delivery.estimation;

import java.util.List;

public abstract class Distribution {
	protected List<Double> i1, i2, c;
	protected double[] ao, io;
	protected static int K;
	protected int degrees;
	private final static double lambda = 2.0;
	private final static double minIo = 1000.0;
	
	protected Distribution(List<Double> i1, List<Double> i2, List<Double> c, double[] ao) {
		this.i1 = i1;
		this.i2 = i2;
		this.c = c;
		this.ao = ao;
		K = c.size();
		this.degrees = ao.length;
		double io1 = 0.0;
		double io2 = 0.0;
		for (int k = 0; k < K; k++) {
			io1 += i1.get(k) * weight(k);
			io2 += i2.get(k) * weight(k);
		}
		io1 *= 0.5;
		io2 *= 0.5;
		this.io = new double[] { Math.max(io1, minIo), Math.max(io2, minIo) };
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
