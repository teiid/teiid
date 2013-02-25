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

import org.teiid.adminapi.EngineStatistics;

public class EngineStatisticsMetadata extends AdminObjectImpl implements EngineStatistics {
	private static final long serialVersionUID = -6207621797253332528L;
	
	private int sessionCount;
	private long totalMemoryUsedInKB;
	private long memoryUsedByActivePlansInKB;
	private long diskWriteCount;
	private long diskReadCount;
	private long cacheWriteCount;
	private long cacheReadCount;	
	private long diskSpaceUsedInMB;
	private int activePlanCount;
	private int waitPlanCount;
	private int maxWaitPlanCount;
	
	@Override
	public int getSessionCount() {
		return sessionCount;
	}

	@Override
	public long getTotalMemoryUsedInKB() {
		return totalMemoryUsedInKB;
	}

	@Override
	public long getMemoryUsedByActivePlansInKB() {
		return memoryUsedByActivePlansInKB;
	}

	@Override
	public long getDiskWriteCount() {
		return diskWriteCount;
	}

	@Override
	public long getDiskReadCount() {
		return diskReadCount;
	}

	@Override
	public long getCacheReadCount() {
		return cacheReadCount;
	}

	@Override
	public long getCacheWriteCount() {
		return cacheWriteCount;
	}

	@Override
	public long getDiskSpaceUsedInMB() {
		return diskSpaceUsedInMB;
	}

	@Override
	public int getActivePlanCount() {
		return activePlanCount;
	}

	@Override
	public int getWaitPlanCount() {
		return waitPlanCount;
	}

	@Override
	public int getMaxWaitPlanWaterMark() {
		return maxWaitPlanCount;
	}

	public void setSessionCount(int sessionCount) {
		this.sessionCount = sessionCount;
	}

	public void setTotalMemoryUsedInKB(long totalMemoryUsedInKB) {
		this.totalMemoryUsedInKB = totalMemoryUsedInKB;
	}

	public void setMemoryUsedByActivePlansInKB(long memoryUsedByActivePlansInKB) {
		this.memoryUsedByActivePlansInKB = memoryUsedByActivePlansInKB;
	}

	public void setDiskWriteCount(long diskWriteCount) {
		this.diskWriteCount = diskWriteCount;
	}

	public void setDiskReadCount(long diskReadCount) {
		this.diskReadCount = diskReadCount;
	}

	public void setCacheWriteCount(long cacheWriteCount) {
		this.cacheWriteCount = cacheWriteCount;
	}

	public void setCacheReadCount(long cacheReadCount) {
		this.cacheReadCount = cacheReadCount;
	}

	public void setDiskSpaceUsedInMB(long diskSpaceUsedInMB) {
		this.diskSpaceUsedInMB = diskSpaceUsedInMB;
	}

	public void setActivePlanCount(int activePlanCount) {
		this.activePlanCount = activePlanCount;
	}

	public void setWaitPlanCount(int waitPlanCount) {
		this.waitPlanCount = waitPlanCount;
	}

	public void setMaxWaitPlanWaterMark(int maxWaitPlanCount) {
		this.maxWaitPlanCount = maxWaitPlanCount;
	}

}
