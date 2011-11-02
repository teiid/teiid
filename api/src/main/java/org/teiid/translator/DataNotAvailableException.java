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
package org.teiid.translator;

import org.teiid.core.TeiidRuntimeException;

/**
 * Used by asynch connectors to indicate data is not available 
 * and results should be polled for after the given delay in milliseconds.
 */
public class DataNotAvailableException extends TeiidRuntimeException {

	private static final long serialVersionUID = 5569111182915674334L;

	private long retryDelay = 0;
	
	/**
	 * Indicate that the engine should not poll for results and will be notified
	 * via the {@link ExecutionContext#dataAvailable()} method.
	 */
	public static final DataNotAvailableException NO_POLLING = new DataNotAvailableException(-1);
	
	/**
	 * Uses a delay of 0, which implies an immediate poll for results.
	 */
	public DataNotAvailableException() {
	}
	
	/**
	 * Uses the given retryDelay.  Negative values indicate that the
	 * engine should not poll for results (see also {@link DataNotAvailableException#NO_POLLING} and will be notified
	 * via the {@link ExecutionContext#dataAvailable()} method.
	 * @param retryDelay in milliseconds
	 */
	public DataNotAvailableException(long retryDelay) {
		this.retryDelay = retryDelay;
	}
	
	public long getRetryDelay() {
		return retryDelay;
	}

}
