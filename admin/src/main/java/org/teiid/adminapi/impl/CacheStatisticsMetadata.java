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
package org.teiid.adminapi.impl;

import org.teiid.adminapi.CacheStatistics;

public class CacheStatisticsMetadata extends AdminObjectImpl implements CacheStatistics{

	private static final long serialVersionUID = -3514505497661004560L;
	
	private double hitRatio;
	private int totalEntries;
	private int requestCount;
	
	@Override
	public int getRequestCount() {
		return requestCount;
	}

	public void setRequestCount(int count) {
		this.requestCount = count;
	}

	@Override
	public double getHitRatio() {
		return this.hitRatio;
	}

	@Override
	public int getTotalEntries() {
		return this.totalEntries;
	}

	public void setHitRatio(double value) {
		this.hitRatio = value;
	}

	public void setTotalEntries(int value) {
		this.totalEntries = value;
	}	
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("hitRatio=").append(hitRatio);//$NON-NLS-1$
		sb.append("; totalEntries=").append(totalEntries); //$NON-NLS-1$
		sb.append("; requestCount=").append(requestCount); //$NON-NLS-1$
		return sb.toString();
	}
}
