package com.sharethis.delivery.estimation;

import java.util.List;

import com.sharethis.delivery.solvers.Minimizer;
import com.sharethis.delivery.solvers.Solver;
import com.sharethis.delivery.solvers.SolverException;

public class StretchedExponentialDistribution extends Distribution implements Function, Likelihood {
	private final double exponent = 1.5;

	private StretchedExponentialDistribution(List<Double> i1, List<Double> i2, List<Double> c, double[] ao) {
		super(i1, i2, c, ao);
	}

	public double f(double[] a) {
		double f = (Math.pow(io[0] * Math.abs(a[0] - ao[0]), exponent) + Math.pow(io[1] * Math.abs(a[1] - ao[1]),
				exponent)) / exponent;
		for (int k = 0; k < K; k++) {
			double mu = a[0] * i1.get(k) + a[1] * i2.get(k);
			f += Math.pow(Math.abs(mu - c.get(k)), exponent) * weight(k) / exponent;
		}
		return f;
	}

	public double[] df(double[] a) {
		double[] S = new double[degrees];
		double sign1 = (a[0] < ao[0] ? -1.0 : 1.0);
		double sign2 = (a[1] < ao[1] ? -1.0 : 1.0);
		S[0] = sign1 * io[0] * Math.pow(io[0] * Math.abs(a[0] - ao[0]), exponent - 1.0);
		S[1] = sign2 * io[1] * Math.pow(io[1] * Math.abs(a[1] - ao[1]), exponent - 1.0);
		for (int k = 0; k < K; k++) {
			double dmu = a[0] * i1.get(k) + a[1] * i2.get(k) - c.get(k);
			double sign = (dmu < 0 ? -1.0 : 1.0);
			S[0] += sign * Math.pow(Math.abs(dmu), exponent - 1.0) * i1.get(k) * weight(k);
			S[1] += sign * Math.pow(Math.abs(dmu), exponent - 1.0) * i2.get(k) * weight(k);
		}
		return S;
	}

	public double[] maximize() throws SolverException {
		Solver solver = new Minimizer(100, false, "");
		Function f = new StretchedExponentialDistribution(i1, i2, c, ao);
		return solver.minimize(f, ao);
	}

	public static double[] maximizeLikelihood(List<Double> i1, List<Double> i2, List<Double> c, double[] ao)
			throws SolverException {
		Likelihood likelihood = new StretchedExponentialDistribution(i1, i2, c, ao);
		double[] a = likelihood.maximize();
		return new double[] { a[0], a[1], 0.0 };
	}
}
