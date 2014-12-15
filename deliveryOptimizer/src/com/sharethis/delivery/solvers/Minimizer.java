package com.sharethis.delivery.solvers;

import com.sharethis.delivery.estimation.Function;
import com.sharethis.delivery.solvers.LBFGS.ExceptionWithIflag;

public class Minimizer implements Solver {

	// private Function fun;
	// private double[] x, x0;
	int ITMAX;
	int[] iprint = new int[2];
	String campaignName;

	public Minimizer() {
	}

	public Minimizer(int itmax, boolean printOptimizerLogs, String campaignName) {
		ITMAX = itmax;
		this.campaignName = campaignName;
		if (printOptimizerLogs) {
			iprint[0] = 1;
			iprint[1] = 0;
		} else {
			iprint[0] = -1;
			iprint[1] = -1;
		}
	}

	public void setItmax(int itmax) {
		ITMAX = itmax;
	}

	public double[] minimize(Function fun, double[] x0) throws SolverException {
		// this.fun= fun;
		// this.x0= x0;
		int n = x0.length;
		int ndim = n;

		double[] x = new double[ndim];

		for (int i = 0; i < n; i++) {
			x[i] = x0[i];
		}

		int msave = 7;
		int nwork = ndim * (2 * msave + 1) + 2 * msave;

		double g[], diag[], w[];
		// x = new double [ ndim ];
		g = new double[ndim];
		diag = new double[ndim];
		w = new double[nwork];

		double f, eps, xtol, gtol, t1, t2, stpmin, stpmax;
		int iflag[] = new int[1], icall, m, mp, lp, j;
		boolean diagco;

		m = 5;
		
		diagco = false;
		eps = 1.0e-3;
		xtol = 1.0e-12;
		icall = 0;
		iflag[0] = 0;

		do {
			f = fun.f(x);
			g = fun.df(x);

			try {
				LBFGS.lbfgs(n, m, x, f, g, diagco, diag, iprint, eps, xtol, iflag, campaignName);
			} catch (LBFGS.ExceptionWithIflag e) {
				throw new SolverException("Minimize.Sdrive: lbfgs failed: ", e);
			}

			icall += 1;
		} while (iflag[0] != 0 && icall <= ITMAX);

		return x;
	}

}