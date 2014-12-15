package com.sharethis.delivery.estimation;

public interface Function {
	public double f(double[] r);

	public double[] df(double[] r);
}
