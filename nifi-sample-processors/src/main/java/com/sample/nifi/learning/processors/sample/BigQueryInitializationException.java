package com.sample.nifi.learning.processors.sample;

public class BigQueryInitializationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 76763368754L;

	public BigQueryInitializationException(String message, Throwable cause) {
		super(message, cause);
	}

	public BigQueryInitializationException(String message) {
		super(message);
	}
}
