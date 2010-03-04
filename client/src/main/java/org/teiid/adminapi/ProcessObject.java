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

import java.net.InetAddress;
import java.util.Date;


/** 
 * A Process in the Teiid System
 * 
 * <p>The identifier pattern for a Process is <code>"processName"</code>.
 * This Process identifier is considered to be unique across the system.</p>
 * @since 4.3
 */
public interface ProcessObject extends
                        AdminObject {
    
  
    /**
     * Get the Host name 
     *  
     * @return String host name where the process is running
     * @since 4.3
     */
    public String getHostName();	
	
	/**
     * Get the Process name 
     *  
     * @return String A unique identifier for this Process.
     * @since 4.3
     */
    public String getProcessName();


    /**
     * Get the port number for this MetaMatrix Process
     *  
     * @return listener port for this host
     * @since 4.3
     */
    public int getPort();
    
    /**
     * Get the IP address for the MetaMatrix Process 
     * @return the IP address for the MetaMatrix Process 
     * @since 4.3
     */
    public InetAddress getInetAddress(); 
       
    /**
     * Is this process enabled in Configuration
     *  
     * @return whether this process is enabled.
     * @since 4.3
     */
    public boolean isEnabled();

    /**
     * @return amount of free memory for this Java process.
     */
    public long getFreeMemory();    
    
    
    /**
     * @return thread count for this Java process.
     */
    public int getThreadCount();

    /**
     * @return total memory allocated for this Java process.
     */
    public long getTotalMemory();

    
    /**
     * @return whether this process is running.
     * @since 4.3
     */
    public boolean isRunning();
      
    /** 
     * @return Returns the objectsRead.
     * @since 4.3
     */
    public long getObjectsRead();
 
    /** 
     * @return Returns the objectsWritten.
     * @since 4.3
     */
    public long getObjectsWritten();
    
    
    /** 
     * @return Returns the maxSockets.
     * @since 4.3
     */
    public int getMaxSockets();
        
    /** 
     * @return Returns the sockets.
     * @since 4.3
     */
    public int getSockets();
        
    /** 
     * @return Returns the startTime.
     * @since 4.3
     */
    public Date getStartTime();
    
    
    /** 
     * @return Returns the queueWorkerPool.
     * @since 4.3
     */
    public WorkerPoolStatistics getQueueWorkerPool();
}
