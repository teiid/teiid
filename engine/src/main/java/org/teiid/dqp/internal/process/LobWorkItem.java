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

package org.teiid.dqp.internal.process;

import java.io.IOException;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BlockedOnMemoryException;
import com.metamatrix.common.buffer.MemoryNotAvailableException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.lob.LobChunk;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.util.LogConstants;

public class LobWorkItem implements Runnable {
	
	private RequestWorkItem parent;
    private RequestID requestID; 
    
	private DQPCore dqpCore;
	private int chunkSize; 
    
	/* private work item state */
	private String streamId; 
    private LobChunkStream stream;
    private int streamRequestId;
    private ResultsReceiver<LobChunk> resultsReceiver;
	
	public LobWorkItem(RequestWorkItem parent, DQPCore dqpCore, String streamId, int streamRequestId) {
		this.chunkSize = dqpCore.getChunkSize();
		this.dqpCore = dqpCore;
		this.requestID = parent.requestID;
		this.streamId = streamId;
		this.parent = parent;
		this.streamRequestId = streamRequestId;
	}

	public void run() {
		LobChunk chunk = null;
		Exception ex = null;
		boolean shouldClose = false;
		
    	try {
        	// If no previous stream is not found for this request create one and 
            // save for future 
            if (stream == null) {
                stream = createLobStream(new TupleSourceID(streamId));
            }
            
            // now get the chunk from stream
            chunk = stream.getNextChunk();
            shouldClose = chunk.isLast();
    	} catch (BlockedOnMemoryException e) {
			LogManager.logDetail(LogConstants.CTX_DQP, new Object[] {"Reenqueueing LOB chunk request due to lack of available memory ###########", requestID}); //$NON-NLS-1$ //$NON-NLS-2$
			this.dqpCore.addWork(this);
			return;
    	} catch (TupleSourceNotFoundException e) {
            LogManager.logWarning(LogConstants.CTX_DQP, e, DQPPlugin.Util.getString("BufferManagerLobChunkStream.no_tuple_source", streamId)); //$NON-NLS-1$
            ex = e;
        } catch (MetaMatrixComponentException e) {            
            LogManager.logWarning(LogConstants.CTX_DQP, e, DQPPlugin.Util.getString("ProcessWorker.LobError")); //$NON-NLS-1$
            ex = e;
        } catch (IOException e) {
			ex = e;
		} 
        
        synchronized (this) {
	        if (ex != null) {
	        	resultsReceiver.exceptionOccurred(ex);
	        	shouldClose = true;
	        } else {
	        	resultsReceiver.receiveResults(chunk);
	        }
	        resultsReceiver = null;
        }
        
        if (shouldClose) {
        	try {
				stream.close();
			} catch (IOException e) {
				LogManager.logWarning(LogConstants.CTX_DQP, e, DQPPlugin.Util.getString("ProcessWorker.LobError")); //$NON-NLS-1$
			}
        	parent.removeLobStream(streamRequestId);
        }
	}    
    
    /**
     * Create a object which can create a sequence of LobChunk objects on a given
     * LOB object 
     */
    private LobChunkStream createLobStream(TupleSourceID referenceStreamId) 
        throws BlockedOnMemoryException, MetaMatrixComponentException, IOException, TupleSourceNotFoundException {
        
        // get the reference object in the buffer manager, and try to stream off
        // the original sources.
        TupleBatch batch = null;
        try {
            batch = dqpCore.getBufferManager().pinTupleBatch(referenceStreamId, 1, 1);
            List[] tuples = batch.getAllTuples();

            if (tuples != null && tuples.length > 0) {
                Object anObj = tuples[0].get(0);
                if (anObj instanceof Streamable) {
                    Streamable streamable = (Streamable)anObj;
                    return new LobChunkStream(streamable, chunkSize, dqpCore.getBufferManager());                        
                }                                    
            } 
            throw new MetaMatrixComponentException(DQPPlugin.Util.getString("ProcessWorker.wrongdata")); //$NON-NLS-1$
        } catch (MemoryNotAvailableException e) {
            throw BlockedOnMemoryException.INSTANCE;
        } finally {
            try {
                if (batch != null) {
                	dqpCore.getBufferManager().unpinTupleBatch(referenceStreamId, batch.getBeginRow(), batch.getEndRow());
                }
            } catch (MetaMatrixComponentException e) {
            	LogManager.logDetail(LogConstants.CTX_DQP, e, "Call to unpin failed during lob stream creation"); //$NON-NLS-1$
            } 
        }        
    }
    
    synchronized void setResultsReceiver(ResultsReceiver<LobChunk> resultsReceiver) {
    	Assertion.isNull(this.resultsReceiver, "Cannot request results with a pending request"); //$NON-NLS-1$
    	this.resultsReceiver = resultsReceiver;
    }
}
