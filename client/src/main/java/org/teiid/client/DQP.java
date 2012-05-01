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

package org.teiid.client;

import javax.transaction.xa.Xid;

import org.teiid.client.lob.LobChunk;
import org.teiid.client.metadata.MetadataResult;
import org.teiid.client.security.Secure;
import org.teiid.client.util.ResultsFuture;
import org.teiid.client.xa.XATransactionException;
import org.teiid.client.xa.XidImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;


public interface DQP {
	
	@Secure(optional=true)
	ResultsFuture<ResultsMessage> executeRequest(long reqID, RequestMessage message) throws TeiidProcessingException, TeiidComponentException;
	
	ResultsFuture<ResultsMessage> processCursorRequest(long reqID, int batchFirst, int fetchSize) throws TeiidProcessingException;

	ResultsFuture<?> closeRequest(long requestID) throws TeiidProcessingException, TeiidComponentException;
	
	boolean cancelRequest(long requestID) throws TeiidProcessingException, TeiidComponentException;
	
	ResultsFuture<?> closeLobChunkStream(int lobRequestId, long requestId, String streamId) throws TeiidProcessingException, TeiidComponentException;
	
	ResultsFuture<LobChunk> requestNextLobChunk(int lobRequestId, long requestId, String streamId) throws TeiidProcessingException, TeiidComponentException;
		
	MetadataResult getMetadata(long requestID) throws TeiidComponentException, TeiidProcessingException;
	
	MetadataResult getMetadata(long requestID, String preparedSql, boolean allowDoubleQuotedVariable) throws TeiidComponentException, TeiidProcessingException;
	
    // local transaction
 
	ResultsFuture<?> begin() throws XATransactionException;

    ResultsFuture<?> commit() throws XATransactionException; 

    ResultsFuture<?> rollback() throws XATransactionException; 

    // XA

    ResultsFuture<?> start(XidImpl xid,
            int flags,
            int timeout) throws XATransactionException;

    ResultsFuture<?> end(XidImpl xid,
            int flags) throws XATransactionException;

    ResultsFuture<Integer> prepare(XidImpl xid) throws XATransactionException;

    ResultsFuture<?> commit(XidImpl xid, boolean onePhase) throws XATransactionException;
    
    ResultsFuture<?> rollback(XidImpl xid) throws XATransactionException;

    ResultsFuture<?> forget(XidImpl xid) throws XATransactionException;

    ResultsFuture<Xid[]> recover(int flag) throws XATransactionException; 
    
}
