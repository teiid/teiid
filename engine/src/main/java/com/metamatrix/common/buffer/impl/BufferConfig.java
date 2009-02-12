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

package com.metamatrix.common.buffer.impl;

import java.util.Properties;

import com.metamatrix.common.buffer.BufferManagerPropertyNames;

/**
 * Encapsulates all configuration information for the BufferManagerImpl, 
 * including both properties that are set and some that are derived.
 */
public class BufferConfig {
	
	public static int DEFAULT_CONNECTOR_BATCH_SIZE = 1000;

    // Configuration 
    private long totalAvailableMemory = 100000000;
    private int groupUsePercentage = 80;
    private int activeMemoryThreshold = 75;
    private int managementInterval = 500;
    private int connectorBatchSize = DEFAULT_CONNECTOR_BATCH_SIZE;
    //private int processorBatchSize = 500;
    private int processorBatchSize = 100;
    private String bufferStorageDirectory = "../buffer"; //$NON-NLS-1$
    private int logStatInterval = 0;
    
    // Derived state 
    private long availableSessionLevel = 0;
    private long activeMemoryLevel = 0;

    /**
     * Constructor for BufferConfig - use all defaults
     */
    public BufferConfig() {
        computeDerived();
    }

    /**
     * Constructor for BufferConfig - set from properties.
     * @param props Properties as defined in 
     * {@link com.metamatrix.common.buffer.BufferManagerPropertyNames}.
     */
    public BufferConfig(Properties props) {
        // Read totalAvailableMemory
        try {
            String propStr = props.getProperty(BufferManagerPropertyNames.MEMORY_AVAILABLE);    
            if(propStr != null) {
                totalAvailableMemory = Integer.parseInt(propStr) * 1000000L;
            }
        } catch(NumberFormatException e) {
            // use default
        }

        // Read groupUsePercentage
        try {
            String propStr = props.getProperty(BufferManagerPropertyNames.SESSION_USE_PERCENTAGE);    
            if(propStr != null) {
                groupUsePercentage = Integer.parseInt(propStr);
            }
        } catch(NumberFormatException e) {
            // use default
        }

        // Read activeMemoryThreshold
        try {
            String propStr = props.getProperty(BufferManagerPropertyNames.ACTIVE_MEMORY_THRESHOLD);    
            if(propStr != null) {
                activeMemoryThreshold = Integer.parseInt(propStr);
            }
        } catch(NumberFormatException e) {
            // use default
        }
        
        // Read managementInterval
        try {
            String propStr = props.getProperty(BufferManagerPropertyNames.MANAGEMENT_INTERVAL);    
            if(propStr != null) {
                managementInterval = Integer.parseInt(propStr);
            }
        } catch(NumberFormatException e) {
            // use default
        }

        // Read connectorBatchSize
        try {
            String propStr = props.getProperty(BufferManagerPropertyNames.CONNECTOR_BATCH_SIZE);    
            if(propStr != null) {
                connectorBatchSize = Integer.parseInt(propStr);
            }
        } catch(NumberFormatException e) {
            // use default
        }
        
        // Read processorBatchSize
        try {
            String propStr = props.getProperty(BufferManagerPropertyNames.PROCESSOR_BATCH_SIZE);    
            if(propStr != null) {
                processorBatchSize = Integer.parseInt(propStr);
            }
        } catch(NumberFormatException e) {
            // use default
        }

        // Read bufferStorageDirectory
        try {
            String propStr = props.getProperty(BufferManagerPropertyNames.BUFFER_STORAGE_DIRECTORY);    
            if(propStr != null) {
                bufferStorageDirectory = propStr;
            }
        } catch(NumberFormatException e) {
            // use default
        }
        
        // Read logStatInterval
        try {
            String propStr = props.getProperty(BufferManagerPropertyNames.LOG_STATS_INTERVAL);    
            if(propStr != null) {
                logStatInterval = Integer.parseInt(propStr);
            }
        } catch(NumberFormatException e) {
            // use default
        }

        computeDerived();
    }

    // Direct state management

    public long getTotalAvailableMemory() {
        return this.totalAvailableMemory;
    }

    public void setTotalAvailableMemory(long totalAvailableMemory) {
        this.totalAvailableMemory = totalAvailableMemory;        
        computeDerived();
    }
    
    public int getGroupUsePercentage() {
        return this.groupUsePercentage;
    } 

    public void setGroupUsePercentage(int groupUsePercentage) {
        this.groupUsePercentage = groupUsePercentage;
        computeDerived();
    } 

    public int getActiveMemoryThreshold() {
        return this.activeMemoryThreshold;
    } 
    
    public void setActiveMemoryThreshold(int activeMemoryThreshold) {
        this.activeMemoryThreshold = activeMemoryThreshold;
        computeDerived();
    } 

    public int getConnectorBatchSize() {
        return this.connectorBatchSize;
    } 

    public void setConnectorBatchSize(int connectorBatchSize) {
        this.connectorBatchSize = connectorBatchSize;
    } 

    public int getProcessorBatchSize() {
        return this.processorBatchSize;
    } 

    public void setProcessorBatchSize(int processorBatchSize) {
        this.processorBatchSize = processorBatchSize;
    } 

    public int getManagementInterval() {
        return this.managementInterval;
    } 
    
    public void setManagementInterval(int managementInterval) {
        this.managementInterval = managementInterval;
    } 

    public String getBufferStorageDirectory() {
        return this.bufferStorageDirectory;
    } 
    
    public void setBufferStorageDirectory(String bufferStorageDirectory) {
        this.bufferStorageDirectory = bufferStorageDirectory;
    } 
    
    public int getLogStatInterval() {
        return this.logStatInterval;
    }
    
    public void setStatUpdateInterval(int logStatInterval) {
        this.logStatInterval = logStatInterval;
    }

    // Derived state management
    
    private void computeDerived() {
        this.availableSessionLevel = (int) (this.totalAvailableMemory * (this.groupUsePercentage / 100.0));    
        this.activeMemoryLevel = (int) (this.totalAvailableMemory * (this.activeMemoryThreshold / 100.0));    
    }
    
    public long getMaxAvailableSession() {
        return this.availableSessionLevel;
    }
     
    public long getActiveMemoryLevel() {
        return this.activeMemoryLevel;
    }

}
