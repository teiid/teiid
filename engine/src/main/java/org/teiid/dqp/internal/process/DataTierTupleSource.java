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

import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.ConnectorException;
import org.teiid.dqp.internal.datamgr.impl.ConnectorWork;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.exception.SourceWarning;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;

/**
 * This tuple source impl can only be used once; once it is closed, it 
 * cannot be reopened and reused.
 */
public class DataTierTupleSource implements TupleSource {

    // Construction state
    private final List schema;
    private final AtomicRequestMessage aqr;
    private final DataTierManagerImpl dataMgr;
    private final String connectorName;
    private final RequestWorkItem workItem;
    
    // Data state
    private ConnectorWork cwi;
    private int index;
    private int rowsProcessed;
    private AtomicResultsMessage arm;
    private boolean closed;
    
    /**
     * Constructor for DataTierTupleSource.
     */
    public DataTierTupleSource(List schema, AtomicRequestMessage aqr, DataTierManagerImpl dataMgr, String connectorName, RequestWorkItem workItem) {
        this.schema = schema;       
        this.aqr = aqr;
        this.dataMgr = dataMgr;
        this.connectorName = connectorName;
        this.workItem = workItem;
    }

    /**
     * @see TupleSource#getSchema()
     */
    public List getSchema() {
        return this.schema;
    }

    public List nextTuple() throws MetaMatrixComponentException, MetaMatrixProcessingException {
    	if (this.arm == null) {
    		open();
    	}
    	while (true) {
	    	if (index < arm.getResults().length) {
	            return this.arm.getResults()[index++];
	        }
	    	if (this.arm.getFinalRow() > 0) {
	    		return null;
	    	}
	    	try {
				receiveResults(this.cwi.more());
			} catch (ConnectorException e) {
	        	exceptionOccurred(e, true);
			}
    	}
    }
    
    void open() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        try {
	        if (this.cwi == null) {
	        	this.cwi = this.dataMgr.executeRequest(aqr, this.connectorName);
	        	Assertion.isNull(workItem.getConnectorRequest(aqr.getAtomicRequestID()));
	            workItem.addConnectorRequest(aqr.getAtomicRequestID(), this);
	        }
	        receiveResults(this.cwi.execute());
        } catch (ConnectorException e) {
        	exceptionOccurred(e, true);
        }
    }
    
    public void fullyCloseSource() {
    	if (!closed) {
    		if (cwi != null) {
		    	workItem.closeAtomicRequest(this.aqr.getAtomicRequestID());
				this.cwi.close();
    		}
			closed = true;
    	}
    }
    
    public void cancelRequest() {
    	if (this.cwi != null) {
    		this.cwi.cancel();
    	}
    }

    /**
     * @see TupleSource#closeSource()
     */
    public void closeSource() {
    	if (this.arm == null || this.arm.supportsImplicitClose()) {
        	fullyCloseSource();
    	}
    }

    void exceptionOccurred(ConnectorException exception, boolean removeState) throws MetaMatrixComponentException, MetaMatrixProcessingException {
    	if (removeState) {
			fullyCloseSource();
		}
    	if(workItem.requestMsg.supportsPartialResults()) {
			AtomicResultsMessage emptyResults = new AtomicResultsMessage(new List[0], null);
			emptyResults.setWarnings(Arrays.asList((Exception)exception));
			emptyResults.setFinalRow(this.rowsProcessed);
			receiveResults(arm);
		} else {
    		if (exception.getCause() instanceof MetaMatrixComponentException) {
    			throw (MetaMatrixComponentException)exception.getCause();
    		}
    		if (exception.getCause() instanceof MetaMatrixProcessingException) {
    			throw (MetaMatrixProcessingException)exception.getCause();
    		}
    		throw new MetaMatrixComponentException(exception);
		}	
	}

	void receiveResults(AtomicResultsMessage response) {
		this.arm = response;
        rowsProcessed += response.getResults().length;
        index = 0;
		if (response.getWarnings() != null) {
			for (Exception warning : response.getWarnings()) {
				SourceWarning sourceFailure = new SourceWarning(this.aqr.getModelName(), aqr.getConnectorName(), warning, true);
		        workItem.addSourceFailureDetails(sourceFailure);
			}
		}
	}
	
	public AtomicRequestMessage getAtomicRequestMessage() {
		return aqr;
	}

	public String getConnectorName() {
		return this.connectorName;
	}
	
	public boolean isTransactional() {
		if (this.arm == null) {
			return false;
		}
		return this.arm.isTransactional();
	}
	
	@Override
	public int available() {
		if (this.arm == null) {
			return 0;
		}
		return this.arm.getResults().length - index;
	}
	
}
