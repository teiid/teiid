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

public interface WorkerPoolStatistics extends AdminObject, DomainAware{
 
	/**
	 * Current active thread count
	 * @return
	 */
	public int getActiveThreads();
	
	/**
	 * Highest Active threads recorded so far
	 * @return
	 */
	public int getHighestActiveThreads();
	 
	 
	 /**
	  * Queue Name
	  * @return
	  */
	 public String getQueueName();
	 
	 
	 /**
	  * Max number of active threads allowed
	  * @return
	  */
	 public int getMaxThreads();
	
	/** 
     * @return Returns the number of requests queued.
     * @since 4.3
     */
    public int getQueued();
    
    /**
     * @return The number of completed tasks
     */
    long getTotalCompleted();
    
   
    /**
     * @return The number of submitted tasks
     */
    long getTotalSubmitted();
    
    /** 
     * @return Returns the highest queue size
     */
    public int getHighestQueued();
}
