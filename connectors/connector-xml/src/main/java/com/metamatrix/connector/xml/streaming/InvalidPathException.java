package com.metamatrix.connector.xml.streaming;

public class InvalidPathException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public InvalidPathException() {
	}

	public InvalidPathException(String message) {
		super(message);
	}

	public InvalidPathException(Throwable cause) {
		super(cause);
	}

	public InvalidPathException(String message, Throwable cause) {
		super(message, cause);
	}

}
