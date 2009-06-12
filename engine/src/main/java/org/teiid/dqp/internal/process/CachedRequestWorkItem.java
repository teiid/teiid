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

import java.util.List;

import javax.transaction.SystemException;

import org.teiid.dqp.internal.cache.CacheResults;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BlockedOnMemoryException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.message.ResultsMessage;
import com.metamatrix.query.sql.lang.Command;

public class CachedRequestWorkItem extends RequestWorkItem {

	public CachedRequestWorkItem(DQPCore dqpCore, RequestMessage requestMsg, Request request, ResultsReceiver<ResultsMessage> receiver, RequestID requestID, DQPWorkContext workContext, Command originalCommand) {
		super(dqpCore, requestMsg, request, receiver, requestID, workContext);
		this.originalCommand = originalCommand;
	}
	
	@Override
	protected void processNew() throws MetaMatrixComponentException, MetaMatrixProcessingException  {
		request.initMetadata();
    	request.validateEntitlement(originalCommand);
    	this.request = null;
	}
	
	@Override
	protected void processMore() throws SystemException, BlockedException,
			MetaMatrixCoreException {
		//do nothing
	}

	
	@Override
	protected void sendResultsIfNeeded(TupleBatch batch)
			throws BlockedOnMemoryException, MetaMatrixComponentException,
			TupleSourceNotFoundException {
		synchronized (this.resultsCursor) {
			if (!this.resultsCursor.resultsRequested) {
				return;
			}
		}
		CacheResults cResult = rsCache.getResults(cid, new int[] {this.resultsCursor.begin, this.resultsCursor.end});
		List results[] = cResult.getResults();
		int firstRow = cResult.getFirstRow();

        ResultsMessage response = createResultsMessage(requestMsg, results, cResult.getElements(), cResult.getAnalysisRecord());
        response.setFirstRow(firstRow);
        response.setLastRow(firstRow + results.length - 1);

        boolean isFinal = cResult.isFinal();
        if(isFinal){
            response.setFinalRow(cResult.getFinalRow());
        }
        response.setPartialResults(!isFinal);
        
        this.resultsCursor.resultsSent();
        this.resultsReceiver.receiveResults(response);
	}
	
	@Override
	protected void attemptClose() {
		this.isClosed = true;
		dqpCore.logMMCommand(this, false, false, -1);
		this.dqpCore.removeRequest(this);
	}
	
}