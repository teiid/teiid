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

package com.metamatrix.dqp.client;

import java.util.List;

import javax.transaction.xa.Xid;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.lob.LobChunk;
import com.metamatrix.common.xa.MMXid;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.message.ResultsMessage;

public interface ClientSideDQP {
	
	ResultsFuture<ResultsMessage> executeRequest(long reqID, RequestMessage message) throws MetaMatrixProcessingException, MetaMatrixComponentException;
	
	ResultsFuture<ResultsMessage> processCursorRequest(long reqID, int batchFirst, int batchLast) throws MetaMatrixProcessingException;

	ResultsFuture<?> closeRequest(long requestID) throws MetaMatrixProcessingException, MetaMatrixComponentException;
	
	void cancelRequest(long requestID) throws MetaMatrixProcessingException, MetaMatrixComponentException;
	
	ResultsFuture<?> closeLobChunkStream(int lobRequestId, long requestId, String streamId) throws MetaMatrixProcessingException, MetaMatrixComponentException;
	
	ResultsFuture<LobChunk> requestNextLobChunk(int lobRequestId, long requestId, String streamId) throws MetaMatrixProcessingException, MetaMatrixComponentException;
		
	List getXmlSchemas(String docName) throws MetaMatrixComponentException, QueryMetadataException;

	MetadataResult getMetadata(long requestID) throws MetaMatrixComponentException, MetaMatrixProcessingException;
	
	MetadataResult getMetadata(long requestID, String preparedSql, boolean allowDoubleQuotedVariable) throws MetaMatrixComponentException, MetaMatrixProcessingException;
	
    // local transaction
    void begin() throws XATransactionException;

    void commit() throws XATransactionException;

    void rollback() throws XATransactionException;

    // XA
    int prepare(MMXid xid) throws XATransactionException;

    void commit(MMXid xid, boolean onePhase) throws XATransactionException;
    
    void rollback(MMXid xid) throws XATransactionException;

    Xid[] recover(int flag) throws XATransactionException;
    
    void forget(MMXid xid) throws XATransactionException;
    
    void start(MMXid xid,
               int flags,
               int timeout) throws XATransactionException;
    
    void end(MMXid xid,
             int flags) throws XATransactionException;

}
