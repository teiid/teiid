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
package com.metamatrix.connector.loopback;

import org.teiid.connector.basic.BasicManagedConnectionFactory;

public class LoopbackManagedConnectionFactory extends BasicManagedConnectionFactory{

	private static final long serialVersionUID = 6698482857582937744L;
	
	private int waitTime = 0;
	private int rowCount = 1;
	private boolean throwError = false;
	private long pollIntervalInMilli = -1;
	
	public int getWaitTime() {
		return waitTime;
	}
	
	public void setWaitTime(Integer waitTime) {
		this.waitTime = waitTime.intValue();
	}
	
	public int getRowCount() {
		return rowCount;
	}
	
	public void setRowCount(Integer rowCount) {
		this.rowCount = rowCount;
	}
	
	public boolean isThrowError() {
		return this.throwError;
	}
	
	public void setThrowError(Boolean error) {
		this.throwError = error.booleanValue();
	}
	
	public long getPollIntervalInMilli() {
		return this.pollIntervalInMilli;
	}
	
	public void setPollIntervalInMilli(Long intervel) {
		this.pollIntervalInMilli = intervel.longValue();
	}
}
