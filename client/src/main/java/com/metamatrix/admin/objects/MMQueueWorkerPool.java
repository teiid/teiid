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

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.admin.api.objects.QueueWorkerPool;

/**
 * Dataholder for all the statistics gathered about a worker pool.
 */
public class MMQueueWorkerPool extends MMAdminObject implements QueueWorkerPool {

    // Current state    
    private int queued = 0;
    private int threads = 0;
    
    // Counts since last stats retrieval
    private int enqueues = 0;
    private int dequeues = 0;
    private int highwaterMark = 0;
    
    // Total counts, never reset
    private int totalHighwaterMark = 0;
    private long totalEnqueues = 0;
    private long totalDequeues = 0;
    
   
    
    
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
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.enqueues") + enqueues); //$NON-NLS-1$
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.dequeues") + dequeues); //$NON-NLS-1$
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.highwaterMark") + highwaterMark);  //$NON-NLS-1$
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.totalEnqueues") + totalEnqueues);  //$NON-NLS-1$
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.totalDequeues") + totalDequeues);  //$NON-NLS-1$
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.totalHighwaterMark") + totalHighwaterMark); //$NON-NLS-1$
        str.append(AdminPlugin.Util.getString("MMQueueWorkerPool.threads") + threads);     //$NON-NLS-1$
        
        return str.toString();
    }

    
    
    
    
    /** 
     * @return Returns the dequeues.
     * @since 4.3
     */
    public int getDequeues() {
        return this.dequeues;
    }
    /** 
     * @param dequeues The dequeues to set.
     * @since 4.3
     */
    public void setDequeues(int dequeues) {
        this.dequeues = dequeues;
    }
    /** 
     * @return Returns the enqueues.
     * @since 4.3
     */
    public int getEnqueues() {
        return this.enqueues;
    }
    /** 
     * @param enqueues The enqueues to set.
     * @since 4.3
     */
    public void setEnqueues(int enqueues) {
        this.enqueues = enqueues;
    }
    /** 
     * @return Returns the highwaterMark.
     * @since 4.3
     */
    public int getHighwaterMark() {
        return this.highwaterMark;
    }
    /** 
     * @param highwaterMark The highwaterMark to set.
     * @since 4.3
     */
    public void setHighwaterMark(int highwaterMark) {
        this.highwaterMark = highwaterMark;
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
        return this.totalDequeues;
    }
    /** 
     * @param totalDequeues The number of totalDequeues to set.
     * @since 4.3
     */
    public void setTotalDequeues(long totalDequeues) {
        this.totalDequeues = totalDequeues;
    }
    /** 
     * @return Returns the number of totalEnqueues.
     * @since 4.3
     */
    public long getTotalEnqueues() {
        return this.totalEnqueues;
    }
    /** 
     * @param totalEnqueues The number of totalEnqueues to set.
     * @since 4.3
     */
    public void setTotalEnqueues(long totalEnqueues) {
        this.totalEnqueues = totalEnqueues;
    }
    /** 
     * @return Returns the totalHighwaterMark.
     * @since 4.3
     */
    public int getTotalHighwaterMark() {
        return this.totalHighwaterMark;
    }
    /** 
     * @param totalHighwaterMark The totalHighwaterMark to set.
     * @since 4.3
     */
    public void setTotalHighwaterMark(int totalHighwaterMark) {
        this.totalHighwaterMark = totalHighwaterMark;
    }
}

