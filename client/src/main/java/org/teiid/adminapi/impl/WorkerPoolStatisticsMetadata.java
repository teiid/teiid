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

import org.jboss.managed.api.annotation.ManagementProperty;
import org.jboss.metatype.api.annotations.MetaMapping;
import org.teiid.adminapi.WorkerPoolStatistics;

/**
 * This class is a holder for all the statistics gathered about a worker pool.
 */
@MetaMapping(WorkerPoolStatisticsMetadataMapper.class)
public class WorkerPoolStatisticsMetadata extends AdminObjectImpl implements WorkerPoolStatistics {

	private static final long serialVersionUID = -4917902925523802295L;

    // Current state    
    private int queued;
    private int highestQueued;
    private int maxThreads;
    private int activeThreads;
    private int highestActiveThreads;
    private long totalSubmitted;
    private long totalCompleted;
            
    @Override
    @ManagementProperty(description="Number of Active Threads", readOnly=true)
    public int getActiveThreads() {
		return activeThreads;
	}
    
    @Override
    @ManagementProperty(description="Highest active threads", readOnly=true)
    public int getHighestActiveThreads() {
		return highestActiveThreads;
	}
    
    @Override
    @ManagementProperty(description="Total Completed Tasks", readOnly=true)
    public long getTotalCompleted() {
		return totalCompleted;
	}
    
    @Override
    @ManagementProperty(description="Total submitted Tasks", readOnly=true)
    public long getTotalSubmitted() {
		return totalSubmitted;
	}
    
    @Override
    @ManagementProperty(description="Queue Name", readOnly=true)
    public String getQueueName() {
		return getName();
	}
    
    @Override
    @ManagementProperty(description="Currently Queued Tasks", readOnly=true)
    public int getQueued() {
		return queued;
	}
    
    @Override
    @ManagementProperty(description="Highest Queued Tasks", readOnly=true)
    public int getHighestQueued() {
		return highestQueued;
	}
    
    @Override
    @ManagementProperty(description="Max Threads", readOnly=true)
    public int getMaxThreads() {
		return maxThreads;
	}

	public void setQueued(int queued) {
		this.queued = queued;
	}

	public void setHighestQueued(int highestQueued) {
		this.highestQueued = highestQueued;
	}

	public void setMaxThreads(int maxThreads) {
		this.maxThreads = maxThreads;
	}

	public void setActiveThreads(int activeThreads) {
		this.activeThreads = activeThreads;
	}

	public void setHighestActiveThreads(int highestActiveThreads) {
		this.highestActiveThreads = highestActiveThreads;
	}

	public void setTotalSubmitted(long totalSubmitted) {
		this.totalSubmitted = totalSubmitted;
	}

	public void setTotalCompleted(long totalCompleted) {
		this.totalCompleted = totalCompleted;
	}
	
    public void setQueueName(String name) {
		setName(name);
	}	
    
    public String toString() {
    	StringBuilder str = new StringBuilder();
        
        str.append("WorkerPoolStats:"); //$NON-NLS-1$
        str.append("  queue-name = " + getName()); //$NON-NLS-1$
        str.append("; queued = " + queued); //$NON-NLS-1$
        str.append("; highestQueued = " + highestQueued); //$NON-NLS-1$
        str.append("; maxThreads = " + maxThreads);     //$NON-NLS-1$
        str.append("; activeThreads = " + activeThreads);     //$NON-NLS-1$
        str.append("; highestActiveThreads = " + highestActiveThreads);     //$NON-NLS-1$
        str.append("; totalSubmitted = " + totalSubmitted);     //$NON-NLS-1$
        str.append("; totalCompleted = " + totalCompleted);     //$NON-NLS-1$
        return str.toString();
    }    

}

