package com.metamatrix.connector.api;

import com.metamatrix.connector.exception.ConnectorException;

public class DataNotAvailableException extends ConnectorException {

	private long retryDelay = 0;
	
	public DataNotAvailableException() {
	}
	
	public DataNotAvailableException(long retryDelay) {
		this.retryDelay = retryDelay;
	}
	
	public long getRetryDelay() {
		return retryDelay;
	}

}
