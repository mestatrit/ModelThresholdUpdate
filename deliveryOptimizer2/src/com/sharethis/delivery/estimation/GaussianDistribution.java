package com.sharethis.delivery.estimation;

import java.util.List;
import java.util.Map;

import com.sharethis.delivery.solvers.SolverException;

public class GaussianDistribution extends Distribution implements Function, Likelihood {

	private GaussianDistribution(Map<Integer, List<Double>> i, Map<Integer, List<Double>> c, double[] ao, double[] wo) {
		super(i, c, ao, wo);
	}

	public double f(double[] a) {
		double f = 0.0;
		for (int p = 0; p < degrees; p++) {
			List<Double> ip = i.get(p);
			List<Double> cp = c.get(isConstraintPerDegree ? p : 0);
			for (int k = 0; k < K; k++) {
				if (cp.get(k) == null) continue;
				double mu = a[p] * ip.get(k);
				f += 0.5 * Math.pow(mu - cp.get(k), 2.0) * weight(k);
			}
			f += 0.5 * Math.pow(io[p] * (a[p] - ao[p]), 2.0);
		}
		return f;
	}

	public double[] df(double[] a) {
		double[] S = new double[degrees];
		for (int p = 0; p < degrees; p++) {
			List<Double> ip = i.get(p);
			List<Double> cp = c.get(isConstraintPerDegree ? p : 0);
			S[0] = io[p] * io[p] * (a[p] - ao[p]);
			for (int k = 0; k < K; k++) {
				if (cp.get(k) == null) continue;
				double mu = a[p] * ip.get(k);
				S[p] += (mu - cp.get(k)) * ip.get(k) * weight(k);
			}
		}
		return S;
	}

	public double[] maximize() {
		double[] a;
		if (!isConstraintPerDegree) {
			a = minimize2();
			if (a[1] < 0) {
				a = new double[] { minimize1(0, 0), 0.0, 0.0 };
			} else if (a[0] < 0) {
				a = new double[] { 0.0, minimize1(1, 0), 0.0 };
			}
		} else {
			a = new double[degrees + 1];
			for (int p = 0; p < degrees; p++) {
				a[p] = minimize1(p, p);
			}
		}
		return a;
	}

	private double minimize1(int p, int q) {
		List<Double> ip = i.get(p);
		List<Double> cq = c.get(q);
		double mpp = io[p] * io[p];
		double icp = io[p] * io[p] * ao[p];
		for (int k = 0; k < K; k++) {
			if (cq.get(k) == null) continue;
			mpp += weight(k) * ip.get(k) * ip.get(k);
			icp += weight(k) * ip.get(k) * cq.get(k);
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
			if (c.get(0).get(k) == null) continue;
			m11 += weight(k) * i.get(0).get(k) * i.get(0).get(k);
			m12 += weight(k) * i.get(0).get(k) * i.get(1).get(k);
			m21 += weight(k) * i.get(1).get(k) * i.get(0).get(k);
			m22 += weight(k) * i.get(1).get(k) * i.get(1).get(k);
			ic1 += weight(k) * i.get(0).get(k) * c.get(0).get(k);
			ic2 += weight(k) * i.get(1).get(k) * c.get(0).get(k);
		}
		double det = m11 * m22 - m12 * m21;
		double m11inv =  m22 / det;
		double m22inv =  m11 / det;
		double m12inv = -m12 / det;
		double m21inv = -m21 / det;
		double singularity = (m11 + m22 + Math.sqrt((m11 - m22) * (m11 - m22) + 4.0 * m12 * m21))
				/ (m11 + m22 - Math.sqrt((m11 - m22) * (m11 - m22) + 4.0 * m12 * m21));

		double a1p = m11inv * ic1 + m12inv * ic2;
		double a2p = m21inv * ic1 + m22inv * ic2;

		return new double[] { a1p, a2p, singularity };
	}

	public static double[] maximizeLikelihood(Map<Integer, List<Double>> i, Map<Integer, List<Double>> c, double[] ao, double[] wo)
			throws SolverException {		
		Likelihood likelihood = new GaussianDistribution(i, c, ao, wo);
		return likelihood.maximize();
	}
}
