package com.sharethis.delivery.estimation;

import java.util.List;

import com.sharethis.delivery.solvers.SolverException;

public class GaussianDistribution extends Distribution implements Function, Likelihood {

	private GaussianDistribution(List<Double> i1, List<Double> i2, List<Double> c, double[] ao) {
		super(i1, i2, c, ao);
	}

	public double f(double[] a) {
		double f = 0.5 * (Math.pow(io[0] * (a[0] - ao[0]), 2.0) + Math.pow(io[1] * (a[1] - ao[1]), 2.0));
		for (int k = 0; k < K; k++) {
			double mu = a[0] * i1.get(k) + a[1] * i2.get(k);
			f += 0.5 * Math.pow(mu - c.get(k), 2.0) * weight(k);
		}
		return f;
	}

	public double[] df(double[] a) {
		double[] S = new double[degrees];
		S[0] = io[0] * io[0] * (a[0] - ao[0]);
		S[1] = io[1] * io[1] * (a[1] - ao[1]);
		for (int k = 0; k < K; k++) {
			double mu = a[0] * i1.get(k) + a[1] * i2.get(k);
			S[0] += (mu - c.get(k)) * i1.get(k) * weight(k);
			S[1] += (mu - c.get(k)) * i2.get(k) * weight(k);
		}
		return S;
	}

	public double[] maximize() {
		double[] a = minimize2();
		if (a[1] < 0) {
			a = new double[] { minimize1(0), 0.0, 0.0 };
		} else if (a[0] < 0) {
			a = new double[] { 0.0, minimize1(1), 0.0 };
		}
		return a;
	}

	private double minimize1(int p) {
		double mpp = io[p] * io[p];
		double icp = io[p] * io[p] * ao[p];
		if (p == 0) {
			for (int k = 0; k < K; k++) {
				mpp += weight(k) * i1.get(k) * i1.get(k);
				icp += weight(k) * i1.get(k) * c.get(k);
			}
		} else {
			for (int k = 0; k < K; k++) {
				mpp += weight(k) * i2.get(k) * i2.get(k);
				icp += weight(k) * i2.get(k) * c.get(k);
			}
		}
		double ap = icp / mpp;
		return ap;
	}

	private double[] minimize2() {
		double m11, m12, m21, m22, ic1, ic2;
		m11 = io[0] * io[0];
		m22 = io[1] * io[1];
		m12 = m21 = 0.0;
		ic1 = io[0] * io[0] * ao[0];
		ic2 = io[1] * io[1] * ao[1];
		for (int k = 0; k < K; k++) {
			m11 += weight(k) * i1.get(k) * i1.get(k);
			m12 += weight(k) * i1.get(k) * i2.get(k);
			m21 += weight(k) * i2.get(k) * i1.get(k);
			m22 += weight(k) * i2.get(k) * i2.get(k);
			ic1 += weight(k) * i1.get(k) * c.get(k);
			ic2 += weight(k) * i2.get(k) * c.get(k);
		}
		double det = m11 * m22 - m12 * m21;
		double m11inv = m22 / det;
		double m22inv = m11 / det;
		double m12inv = -m12 / det;
		double m21inv = -m21 / det;
		double singularity = (m11 + m22 + Math.sqrt((m11 - m22) * (m11 - m22) + 4.0 * m12 * m21))
				/ (m11 + m22 - Math.sqrt((m11 - m22) * (m11 - m22) + 4.0 * m12 * m21));

		double a1p = m11inv * ic1 + m12inv * ic2;
		double a2p = m21inv * ic1 + m22inv * ic2;

		return new double[] { a1p, a2p, singularity };
	}

	public static double[] maximizeLikelihood(List<Double> i1, List<Double> i2, List<Double> c, double[] ao)
			throws SolverException {
		Likelihood likelihood = new GaussianDistribution(i1, i2, c, ao);
		return likelihood.maximize();
	}
}
