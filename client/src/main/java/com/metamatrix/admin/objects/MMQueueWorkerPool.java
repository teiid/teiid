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

package com.metamatrix.admin.objects;

import org.teiid.adminapi.QueueWorkerPool;

import com.metamatrix.admin.AdminPlugin;

/**
 * Dataholder for all the statistics gathered about a worker pool.
 */
public class MMQueueWorkerPool extends MMAdminObject implements QueueWorkerPool {

    // Current state    
    private int queued = 0;
    private int threads = 0;
    
    // Total counts, never reset
    private int highestThreads;
    private int highestQueued = 0;
    private long totalSubmitted = 0;
    private long totalCompleted = 0;
    
    /**
     * Construct a new MMQueueWorkerPool 
     * @param identifierParts
     * @since 4.3
     */
    public MMQueueWorkerPool(String[] identifierParts) {
        super(identifierParts);
    }
   
    /**
     * Get string for display purposes 
     * @see java.lang.Object#toString()
     * @since 4.3
     */
    public String toString() {
        StringBuffer str = new StringBuffer();
        
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.MMQueueWorkerPool") + getIdentifier()); //$NON-NLS-1$
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.queued") + queued); //$NON-NLS-1$
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.highestQueued") + highestQueued);  //$NON-NLS-1$
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.totalSubmitted") + totalSubmitted); //$NON-NLS-1$
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.totalCompleted") + totalCompleted); //$NON-NLS-1$
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.threads") + threads);     //$NON-NLS-1$
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.highestThreads") + highestThreads);  //$NON-NLS-1$
        
        return str.toString();
    }
    
    /** 
     * @return Returns the number of requests queued.
     * @since 4.3
     */
    public int getQueued() {
        return this.queued;
    }
    /** 
     * @param queued The number of requests queued.
     * @since 4.3
     */
    public void setQueued(int queued) {
        this.queued = queued;
    }
    /** 
     * @return Returns the number of threads.
     * @since 4.3
     */
    public int getThreads() {
        return this.threads;
    }
    /** 
     * @param threads The number of threads to set.
     * @since 4.3
     */
    public void setThreads(int threads) {
        this.threads = threads;
    }
    /** 
     * @return Returns the number of totalDequeues.
     * @since 4.3
     */
    public long getTotalDequeues() {
    	return getTotalCompleted();
    }
    /** 
     * @return Returns the number of totalEnqueues.
     * @since 4.3
     */
    public long getTotalEnqueues() {
        return getTotalSubmitted();
    }
    /** 
     * @return Returns the totalHighwaterMark.
     * @since 4.3
     */
    public int getTotalHighwaterMark() {
        return getHighestQueued();
    }

	public int getHighestThreads() {
		return highestThreads;
	}

	public void setHighestThreads(int highestThreads) {
		this.highestThreads = highestThreads;
	}

	public int getHighestQueued() {
		return highestQueued;
	}

	public void setHighestQueued(int highestQueued) {
		this.highestQueued = highestQueued;
	}

	public long getTotalSubmitted() {
		return totalSubmitted;
	}

	public void setTotalSubmitted(long totalSubmitted) {
		this.totalSubmitted = totalSubmitted;
	}

	public void setTotalCompleted(long totalCompleted) {
		this.totalCompleted = totalCompleted;
	}

	public long getTotalCompleted() {
		return totalCompleted;
	}

}

