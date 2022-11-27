/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
