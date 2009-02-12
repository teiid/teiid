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

package com.metamatrix.admin.api.objects;


/** 
 * All the metamatrix server modules use queue based processing inside them. This
 * object holds the statisics of those queues, as per how many of them queued, dequeued, 
 * processed etc.
 * <p>An identifier for QueueWorkerPool, is nothing but the modules it self, like "DQP", 
 * "QueryService" or Connector Binding names etc.</p> 
 * 
 * @since 4.3
 */
public interface QueueWorkerPool extends AdminObject {
    /** 
     * @return Returns the number of dequeues.
     * @since 4.3
     */
    public int getDequeues();
    /** 
     * @return Returns the number of enqueues.
     * @since 4.3
     */
    public int getEnqueues();
    /** 
     * @return Returns the highwaterMark.
     * @since 4.3
     */
    public int getHighwaterMark();
    /** 
     * @return Returns the number of requests queued.
     * @since 4.3
     */
    public int getQueued();
    
    /** 
     * @return Returns the number of threads.
     * @since 4.3
     */
    public int getThreads();
    
    /** 
     * @return Returns the number of totalDequeues.
     * @since 4.3
     */
    public long getTotalDequeues();
    
    /** 
     * @return Returns the number of totalEnqueues.
     * @since 4.3
     */
    public long getTotalEnqueues();
    
    /** 
     * @return Returns the totalHighwaterMark.
     * @since 4.3
     */
    public int getTotalHighwaterMark();
}
