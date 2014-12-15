package com.sharethis.delivery.estimation;

import java.util.List;

import com.sharethis.delivery.solvers.Minimizer;
import com.sharethis.delivery.solvers.Solver;
import com.sharethis.delivery.solvers.SolverException;

public class PoissonDistribution extends Distribution implements Function, Likelihood {

	private PoissonDistribution(List<Double> i1, List<Double> i2, List<Double> c, double[] ao) {
		super(i1, i2, c, ao);
	}

	public double f(double[] a) {
		double[] aa = new double[] { Math.sqrt(a[0]*a[0] + 1.0e-16), Math.sqrt(a[1]*a[1] + 1.0e-16) };
		double f = 0.5 * (Math.pow(io[0] * (aa[0] - ao[0]), 2.0) + Math.pow(io[1] * (aa[1] - ao[1]), 2.0));
		for (int k = 0; k < K; k++) {
			double mu = aa[0] * i1.get(k) + aa[1] * i2.get(k);
			f += (mu - c.get(k) * Math.log(mu)) * weight(k);
		}
		return f;
	}

	public double[] df(double[] a) {
		double[] aa = new double[] { Math.sqrt(a[0]*a[0] + 1.0e-16), Math.sqrt(a[1]*a[1] + 1.0e-16) };
		double[] S = new double[degrees];
		S[0] = io[0] * io[0] * (aa[0] - ao[0]);
		S[1] = io[1] * io[1] * (aa[1] - ao[1]);
		for (int k = 0; k < K; k++) {
			double mu = aa[0] * i1.get(k) + aa[1] * i2.get(k);
			S[0] += (1.0 - c.get(k) / mu) * i1.get(k) * weight(k);
			S[1] += (1.0 - c.get(k) / mu) * i2.get(k) * weight(k);
		}
		S[0] *= a[0] / Math.sqrt(a[0]*a[0] + 1.0e-16);
		S[1] *= a[1] / Math.sqrt(a[1]*a[1] + 1.0e-16);
		
		return S;
	}

	public double[] maximize() throws SolverException {
		Solver solver = new Minimizer(100, false, "");
		Function f = new PoissonDistribution(i1, i2, c, ao);
		return solver.minimize(f, ao);
	}

	public static double[] maximizeLikelihood(List<Double> i1, List<Double> i2, List<Double> c, double[] ao)
			throws SolverException {
		Likelihood likelihood = new PoissonDistribution(i1, i2, c, ao);
		double[] a = likelihood.maximize();
		return new double[] { a[0], a[1], 0.0 };
	}
}
