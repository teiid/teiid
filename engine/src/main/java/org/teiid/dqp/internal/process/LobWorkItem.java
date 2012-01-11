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
import java.nio.charset.Charset;
import java.sql.SQLException;

import javax.resource.spi.work.Work;

import org.teiid.client.lob.LobChunk;
import org.teiid.client.util.ResultsReceiver;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;


public class LobWorkItem implements Work {
	
	private RequestWorkItem parent;
	private int chunkSize; 
    
	/* private work item state */
	private String streamId; 
    private ByteLobChunkStream stream;
    private int streamRequestId;
    private ResultsReceiver<LobChunk> resultsReceiver;
	
	public LobWorkItem(RequestWorkItem parent, DQPCore dqpCore, String streamId, int streamRequestId) {
		this.chunkSize = dqpCore.getChunkSize();
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
                stream = createLobStream(streamId);
            }
            
            // now get the chunk from stream
            chunk = stream.getNextChunk();
            shouldClose = chunk.isLast();
        } catch (TeiidComponentException e) {            
            LogManager.logWarning(org.teiid.logging.LogConstants.CTX_DQP, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30027));
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
        	close();
        }
	}

	void close() {
		try {
			if (stream != null) {
				stream.close();
			}
		} catch (IOException e) {
			LogManager.logWarning(org.teiid.logging.LogConstants.CTX_DQP, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30027));
		}
		parent.removeLobStream(streamRequestId);
	}    
    
    /**
     * Create a object which can create a sequence of LobChunk objects on a given
     * LOB object 
     */
    private ByteLobChunkStream createLobStream(String referenceStreamId) 
        throws TeiidComponentException, IOException {
        
        // get the reference object in the buffer manager, and try to stream off
        // the original sources.
        Streamable<?> streamable = parent.resultsBuffer.getLobReference(referenceStreamId);
        
        try {
            if (streamable instanceof XMLType) {
                XMLType xml = (XMLType)streamable;
                return new ByteLobChunkStream(xml.getBinaryStream(), chunkSize);
            }
            else if (streamable instanceof ClobType) {
                ClobType clob = (ClobType)streamable;
                return new ByteLobChunkStream(new ReaderInputStream(clob.getCharacterStream(), Charset.forName(Streamable.ENCODING)), chunkSize);            
            } 
            BlobType blob = (BlobType)streamable;
            return new ByteLobChunkStream(blob.getBinaryStream(), chunkSize);                        
        } catch(SQLException e) {
            throw new IOException(e);
        }
    }
    
    synchronized void setResultsReceiver(ResultsReceiver<LobChunk> resultsReceiver) {
    	Assertion.isNull(this.resultsReceiver, "Cannot request results with a pending request"); //$NON-NLS-1$
    	this.resultsReceiver = resultsReceiver;
    }

	@Override
	public void release() {
		
	}
}
