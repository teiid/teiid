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

package org.teiid.client.lob;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.client.DQP;
import org.teiid.core.TeiidException;
import org.teiid.core.types.Streamable;
import org.teiid.jdbc.JDBCPlugin;


public class StreamingLobChunckProducer implements LobChunkProducer {
	
	public static class Factory {
		private final Streamable<?> streamable;
		private final DQP dqp;
		private final long requestId;
		
		public Factory(DQP dqp,
				long requestId, Streamable<?> streamable) {
			super();
			this.dqp = dqp;
			this.requestId = requestId;
			this.streamable = streamable;
		}

		public StreamingLobChunckProducer getLobChunkProducer() {
			return new StreamingLobChunckProducer(dqp, requestId, streamable);
		}
	}
	
	private static AtomicInteger REQUEST_SEQUENCE = new AtomicInteger(0);

	private final Streamable<?> streamable;
	private final DQP dqp;
	private final long requestId;
	private final int streamRequestId = REQUEST_SEQUENCE.getAndIncrement();

	public StreamingLobChunckProducer(DQP dqp, long requestId,
			Streamable<?> streamable) {
		this.dqp = dqp;
		this.requestId = requestId;
		this.streamable = streamable;
	}

	public LobChunk getNextChunk() throws IOException {
	    try {
	    	Future<LobChunk> result = dqp.requestNextLobChunk(streamRequestId, requestId, streamable.getReferenceStreamId());
	    	return result.get();
	    } catch (Exception e) {
	        IOException ex = new IOException(JDBCPlugin.Util.getString("StreamImpl.Unable_to_read_data_from_stream", e.getMessage())); //$NON-NLS-1$
	        ex.initCause(e);
	        throw ex;                        
	    }                
	}

	public void close() throws IOException {
	    try {
	    	dqp.closeLobChunkStream(streamRequestId, requestId, streamable.getReferenceStreamId());
	    } catch (TeiidException e) {
	        IOException ex = new IOException(e.getMessage());
	        ex.initCause(e);
	        throw  ex;
		}                    
	}
}
