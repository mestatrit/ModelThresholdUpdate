package com.sharethis.delivery.base;

@SuppressWarnings("serial")
public class DOException extends Exception {
	public DOException(String msg) {
		super(msg);
	}

	public DOException(Exception e) {
		super(e);
	}

	public DOException(String msg, Exception e) {
		super(msg, e);
	}
}