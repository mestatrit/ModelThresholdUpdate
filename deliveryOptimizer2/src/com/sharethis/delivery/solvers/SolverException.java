package com.sharethis.delivery.solvers;

@SuppressWarnings("serial")
public class SolverException extends Exception{
	public SolverException(String msg) {
		super(msg);
	}
	
	public SolverException(Exception e) {
		super(e);
	}
	
	public SolverException(String msg, Exception e) {
		super(msg + e.getMessage());
	}
}
