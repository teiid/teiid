/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.connector.api;

/**
 * Used by asynch connectors to indicate data is not available 
 * and results should be polled for after the given delay.
 */
public class DataNotAvailableException extends ConnectorException {

	private static final long serialVersionUID = 5569111182915674334L;

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
