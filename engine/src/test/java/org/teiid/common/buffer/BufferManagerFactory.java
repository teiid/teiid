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

package org.teiid.common.buffer;

import org.teiid.common.buffer.impl.BufferFrontedFileStoreCache;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.common.buffer.impl.MemoryStorageManager;
import org.teiid.common.buffer.impl.SplittableStorageManager;
import org.teiid.core.TeiidComponentException;


/**
 * <p>Factory for BufferManager instances.  One method will get
 * a server buffer manager, as it should be instantiated in a running
 * MetaMatrix server.  That BufferManager is configured mostly by the
 * passed in properties.</p>
 *
 * <p>The other method returns a stand-alone, in-memory buffer manager.  This
 * is typically used for either in-memory testing or any time the
 * query processor component is not expected to run out of memory, such as
 * within the modeler.</p>
 */
public class BufferManagerFactory {
	
	private static BufferManager INSTANCE;
	
    /**
     * Helper to get a buffer manager all set up for unmanaged standalone use.  This is
     * typically used for testing or when memory is not an issue.
     * @return BufferManager ready for use
     */
    public static BufferManager getStandaloneBufferManager() {
    	if (INSTANCE == null) {
	        BufferManagerImpl bufferMgr = createBufferManager();
	        INSTANCE = bufferMgr;
    	}

        return INSTANCE;
    }

	public static BufferManagerImpl createBufferManager() {
		return initBufferManager(new BufferManagerImpl());
	}

	public static BufferManagerImpl getTestBufferManager(long bytesAvailable, int procBatchSize, int connectorBatchSize) {
		BufferManagerImpl bufferManager = new BufferManagerImpl();
		bufferManager.setProcessorBatchSize(procBatchSize);
		bufferManager.setConnectorBatchSize(connectorBatchSize);
		bufferManager.setMaxProcessingKB((int) (bytesAvailable/1024));
		bufferManager.setMaxReserveKB((int) (bytesAvailable/1024));
	    return initBufferManager(bufferManager);
	}

	public static BufferManagerImpl getTestBufferManager(long bytesAvailable, int procBatchSize) {
		BufferManagerImpl bufferManager = new BufferManagerImpl();
		bufferManager.setProcessorBatchSize(procBatchSize);
		bufferManager.setMaxProcessingKB((int) (bytesAvailable/1024));
		bufferManager.setMaxReserveKB((int) (bytesAvailable/1024));
		return initBufferManager(bufferManager);
	}

	public static BufferManagerImpl initBufferManager(BufferManagerImpl bufferManager) {
	    try {
			bufferManager.initialize();
			MemoryStorageManager storageManager = new MemoryStorageManager();
			SplittableStorageManager ssm = new SplittableStorageManager(storageManager);
			ssm.setMaxFileSizeDirect(MemoryStorageManager.MAX_FILE_SIZE);
			BufferFrontedFileStoreCache fsc = new BufferFrontedFileStoreCache();
			//use conservative allocations
			fsc.setDirect(false); //allow the space to be GCed easily
			fsc.setMaxStorageObjectSize(1<<20);
			fsc.setMemoryBufferSpace(1<<21);
			fsc.setStorageManager(ssm);
			fsc.initialize();
		    bufferManager.setCache(fsc);
		    return bufferManager;
	    } catch (TeiidComponentException e) {
			throw new RuntimeException(e);
		}
	}

}
