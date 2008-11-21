/*
 * (c) 2008 Varsity Gateway LLC. All rights reserved.
 */
package com.metamatrix.uddi.exception;

/**
 * This exception is thrown when an error occurs while publishing or un-publishing to/from an Uddi Registry.
 */
public class MMUddiException extends Exception {

	public MMUddiException() {
	}

	public MMUddiException( String message ) {
		super(message);
	}

	public MMUddiException( Throwable e ) {
		super(e);
	}

	public MMUddiException( Throwable e,
	                        String message ) {
		super(message, e);
	}
}
