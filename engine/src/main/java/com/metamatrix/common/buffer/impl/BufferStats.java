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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.util.LogConstants;

/**
 * This encapsulates all the statistics for the BufferManagerImpl.  The attribute
 * values are set by direct attribute manipulation (no getter/setters).
 */
public class BufferStats {
    
    // Basic stats
    public long memoryUsed;
    public long memoryFree;
    public int numTupleSources;
    public int numPersistentBatches;
    public int numPinnedBatches;
    public int numUnpinnedBatches;
    public String sLocalDrive;

    // Accumulated stats
    public int pinRequests;
    public int pinSuccesses;
    public int pinnedFromMemory;
    public int numCleanings;
    public long totalCleaned;

    // Pinned batch details
    public List pinnedManagedBatches = new LinkedList();
    
    /**
     * Constructor for BufferStats.
     */
    BufferStats() {
    }
    
    /**
     * Log this set of stats.
     */
    void log() {
        LogManager.logInfo(LogConstants.CTX_BUFFER_MGR, "\n");         //$NON-NLS-1$
        LogManager.logInfo(LogConstants.CTX_BUFFER_MGR, "BUFFER MANAGER STATS");         //$NON-NLS-1$
        LogManager.logInfo(LogConstants.CTX_BUFFER_MGR, "    memoryFree = " + memoryFree);         //$NON-NLS-1$
        LogManager.logInfo(LogConstants.CTX_BUFFER_MGR, "    memoryUsed = " + memoryUsed);         //$NON-NLS-1$
        LogManager.logInfo(LogConstants.CTX_BUFFER_MGR, "    numTupleSources = " + numTupleSources);         //$NON-NLS-1$
        LogManager.logInfo(LogConstants.CTX_BUFFER_MGR, "    numPersistentBatches = " + numPersistentBatches);         //$NON-NLS-1$
        LogManager.logInfo(LogConstants.CTX_BUFFER_MGR, "    numPinnedBatches = " + numPinnedBatches);         //$NON-NLS-1$
        LogManager.logInfo(LogConstants.CTX_BUFFER_MGR, "    numUnpinnedBatches = " + numUnpinnedBatches);         //$NON-NLS-1$
        LogManager.logInfo(LogConstants.CTX_BUFFER_MGR, "    pinRequests = " + pinRequests);         //$NON-NLS-1$
        
        // pinSuccesses / pinRequests
        double pinSuccessRate = (pinRequests > 0) ? (((double)pinSuccesses / (double)pinRequests) * 100) : 100;
        LogManager.logInfo(LogConstants.CTX_BUFFER_MGR, "    pinSuccessRate = " + pinSuccessRate);         //$NON-NLS-1$
        
        // pinnedFromMemory / pinRequests
        double memoryHitRate = (pinRequests > 0) ? (((double)pinnedFromMemory / (double)pinRequests) * 100): 100;
        LogManager.logInfo(LogConstants.CTX_BUFFER_MGR, "    memoryHitRate = " + memoryHitRate);         //$NON-NLS-1$
        LogManager.logInfo(LogConstants.CTX_BUFFER_MGR, "    numCleanings = " + numCleanings);         //$NON-NLS-1$
        
        // totalCleaned / numCleanings
        long avgCleaned = (numCleanings > 0) ? (totalCleaned / numCleanings) : 0;
        LogManager.logInfo(LogConstants.CTX_BUFFER_MGR, "    avgCleaned = " + avgCleaned);         //$NON-NLS-1$

        if ( LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE) ) {
            HashMap stackTraces = new HashMap();
            
            if ( pinnedManagedBatches.isEmpty() ) {
                return;
            }
            
            LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "\n");         //$NON-NLS-1$
            LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "    =========== Pinned Batch Details: ===========");         //$NON-NLS-1$            
            
            int stackNumber = 1;
    
            // pinned batch details
            Iterator it = pinnedManagedBatches.iterator();
            while ( it.hasNext() ) {
                ManagedBatch batch = (ManagedBatch)it.next();
                LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "    TupleSourceID: " + batch.getTupleSourceID() + " Begin: " + batch.getBeginRow() + " End: " + batch.getEndRow()); //$NON-NLS-1$  //$NON-NLS-2$  //$NON-NLS-3$
                
                Integer stackKey = (Integer)stackTraces.get(batch.getCallStack());
                
                boolean isFirst = false;
                
                if (stackKey == null) {
                    isFirst = true;
                    stackKey = new Integer(stackNumber++);
                    stackTraces.put(batch.getCallStack(), stackKey);
                }
                
                LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "        Pinned at: " + batch.getCallStackTimeStamp() + " by call# " + stackKey); //$NON-NLS-1$ //$NON-NLS-2$ 
                if (isFirst) {
                    for (Iterator j = batch.getCallStack().iterator(); j.hasNext();) {
                        LogManager.logTrace( LogConstants.CTX_BUFFER_MGR, "        " + j.next() );         //$NON-NLS-1$                
                    }
                }
            }
        }
    }
        
}
