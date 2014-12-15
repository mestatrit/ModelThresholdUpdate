package com.sharethis.delivery.estimation;

import com.sharethis.delivery.solvers.SolverException;

public interface Likelihood {
	public double[] maximize() throws SolverException;

}
