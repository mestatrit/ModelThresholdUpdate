package com.sharethis.delivery.solvers;

import com.sharethis.delivery.estimation.Function;

public interface Solver {
	public void setItmax(int itmax);
	public double[] minimize(Function f, double[] x) throws SolverException;
}
