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

import java.util.Date;

import org.teiid.core.TeiidRuntimeException;

/**
 * Used by asynch connectors to indicate data is not available and results should be polled for after 
 * the given delay in milliseconds or until a Date is reached.
 * <br>
 * Note that delays are not guaranteed unless {@link #strict} is set to true.  With {@link #strict} false, the delay is the maximum amount 
 * of time before the plan will be re-queued for execution. There are several scenarios that would cause the delay to be shorter, such as 
 * multiple sources where one source returns a shorter delay or if the engine believes more work is to be done before allowing the plan to sit idle.
 * <br>
 */
public class DataNotAvailableException extends TeiidRuntimeException {

	private static final long serialVersionUID = 5569111182915674334L;

	private long retryDelay = 0;
	private Date waitUntil;
	private boolean strict;
	
	/**
	 * Indicate that the engine should not poll for results and will be notified
	 * via the {@link ExecutionContext#dataAvailable()} method.  However the engine may still ask
	 * for results before the dataAvailable is called.
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
	
	/**
	 * Instructs the engine to wait until the Date is met before getting results.  By default this will
	 * be strictly enforced, meaning that no attempt will be made to get results before the given Date.
	 * @param waitUntil
	 */
	public DataNotAvailableException(Date waitUntil) {
		this.waitUntil = waitUntil;
		this.strict = true;
	}
	
	public long getRetryDelay() {
		return retryDelay;
	}
	
	public Date getWaitUntil() {
		return waitUntil;
	}
	
	/**
	 * If the delay or Date is strictly enforced then the execution will not asked for results until
	 * after that time or until {@link ExecutionContext#dataAvailable()} is called.
	 * @return
	 */
	public boolean isStrict() {
		return strict;
	}
	
	public void setStrict(boolean strict) {
		this.strict = strict;
	}

}
