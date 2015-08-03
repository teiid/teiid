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
package org.teiid.adminapi;

public interface EngineStatistics extends AdminObject, DomainAware {
	
	/**
	 * Active Number of Sessions in the engine 
	 * @return
	 */
	int getSessionCount();
	
	/**
	 * Total amount memory used by buffer manager for active queries and cached queries
	 * @return
	 */
	long getTotalMemoryUsedInKB();
	
	/**
	 * Total memory used by buffer manager for active plans
	 * @return
	 */
	long getMemoryUsedByActivePlansInKB();
	
	/**
	 * Number of writes to disk by buffer manager to save the overflow from memory
	 * @return
	 */
	long getDiskWriteCount();
	
	/**
	 * Number reads from the disk used by buffer manager that cache overflowed.
	 * @return
	 */
	long getDiskReadCount();
	
	/**
	 * Total number of cache reads, includes disk and soft-cache references
	 * @return
	 */
	long getCacheReadCount();
	
	/**
	 * Total number of cache writes, includes disk and soft-cache references
	 * @return
	 */
	long getCacheWriteCount();
	
	/**
	 * Disk space used by buffer manager to save overflowed memory contents
	 * @return
	 */
	long getDiskSpaceUsedInMB();
	
	/**
	 * Current active plan count
	 * @return
	 */
	int getActivePlanCount();
	
	/**
	 * Current number of waiting plans in the queue
	 * @return
	 */
	int getWaitPlanCount();
	
	/**
	 * High water mark for the waiting plans
	 * @return
	 */
	int getMaxWaitPlanWaterMark();
	
}
