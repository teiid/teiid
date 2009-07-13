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

package com.metamatrix.dqp.service;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.metadata.runtime.ConnectorMetadata;
import org.teiid.dqp.internal.datamgr.impl.ConnectorWorkItem;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;

/**
 * This data service will automatically generate results when called with a query - basically
 * the same as the old loopback connector.
 */
public class AutoGenDataService extends FakeAbstractService implements DataService {

    // Number of rows that will be generated for each query
    private int rows = 10;
    private SourceCapabilities caps;
    
    /**
     * 
     */
    public AutoGenDataService() {
        this(new BasicSourceCapabilities());
    }
    
    public AutoGenDataService(SourceCapabilities caps) {
    	this.caps = caps;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }
    
    public int getRows() {
        return this.rows;
    }

    /* 
     * @see com.metamatrix.dqp.service.DataService#selectConnector(java.lang.String)
     */
    public ConnectorID selectConnector(String connectorBindingID) {
        return null;
    }
    
	public void executeRequest(AtomicRequestMessage request,
			ConnectorID connector,
			ResultsReceiver<AtomicResultsMessage> resultListener)
			throws MetaMatrixComponentException {
        List projectedSymbols = (request.getCommand()).getProjectedSymbols();               
        List[] results = createResults(projectedSymbols);
                
        AtomicResultsMessage msg = ConnectorWorkItem.createResultsMessage(request, results, projectedSymbols);
        msg.setFinalRow(rows);
        resultListener.receiveResults(msg);
        AtomicResultsMessage closeMsg = ConnectorWorkItem.createResultsMessage(request, results, projectedSymbols);
        closeMsg.setRequestClosed(true);
        resultListener.receiveResults(closeMsg);
    }

    private List[] createResults(List symbols) {
        List[] rows = new List[this.rows];

        for(int i=0; i<this.rows; i++) {        
            List row = new ArrayList();        
            Iterator iter = symbols.iterator();
            while(iter.hasNext()) {
                SingleElementSymbol symbol = (SingleElementSymbol) iter.next();
                Class type = symbol.getType();
                row.add( getValue(type) );
            }
        }   
        
        return rows;
    }

    private static final String STRING_VAL = "ABCDEFG"; //$NON-NLS-1$
    private static final Integer INTEGER_VAL = new Integer(0);
    private static final Long LONG_VAL = new Long(0);
    private static final Float FLOAT_VAL = new Float(0.0);
    private static final Short SHORT_VAL = new Short((short)0);
    private static final Double DOUBLE_VAL = new Double(0.0);
    private static final Character CHAR_VAL = new Character('c');
    private static final Byte BYTE_VAL = new Byte((byte)0);
    private static final Boolean BOOLEAN_VAL = Boolean.FALSE;
    private static final BigInteger BIG_INTEGER_VAL = new BigInteger("0"); //$NON-NLS-1$
    private static final BigDecimal BIG_DECIMAL_VAL = new BigDecimal("0"); //$NON-NLS-1$
    private static final java.sql.Date SQL_DATE_VAL = new java.sql.Date(0);
    private static final java.sql.Time TIME_VAL = new java.sql.Time(0);
    private static final java.sql.Timestamp TIMESTAMP_VAL = new java.sql.Timestamp(0);
    
    private Object getValue(Class<?> type) {
        if(type.equals(DataTypeManager.DefaultDataClasses.STRING)) {
            return STRING_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.INTEGER)) {
            return INTEGER_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.SHORT)) { 
            return SHORT_VAL;    
        } else if(type.equals(DataTypeManager.DefaultDataClasses.LONG)) {
            return LONG_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.FLOAT)) {
            return FLOAT_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.DOUBLE)) {
            return DOUBLE_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.CHAR)) {
            return CHAR_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.BYTE)) {
            return BYTE_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.BOOLEAN)) {
            return BOOLEAN_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.BIG_INTEGER)) {
            return BIG_INTEGER_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.BIG_DECIMAL)) {
            return BIG_DECIMAL_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.DATE)) {
            return SQL_DATE_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.TIME)) {
            return TIME_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
            return TIMESTAMP_VAL;
        } else {
            return null;
        }
    }

	public SourceCapabilities getCapabilities(RequestMessage request,
			DQPWorkContext dqpWorkContext, ConnectorID connector)
			throws MetaMatrixComponentException {
        return caps;
    }
        
    /** 
     * @see com.metamatrix.dqp.service.DataService#startConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void startConnectorBinding(String connectorBindingName) throws ApplicationLifecycleException,
                                                                  MetaMatrixComponentException {
    }

    /** 
     * @see com.metamatrix.dqp.service.DataService#stopConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void stopConnectorBinding(String connectorBindingName) throws ApplicationLifecycleException,
                                                                 MetaMatrixComponentException {
    }

    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectorBindings()
     * @since 4.3
     */
    public List getConnectorBindings() throws MetaMatrixComponentException {
        return null;
    }

    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectorBindingState(java.lang.String)
     * @since 4.3
     */
    public ConnectorStatus getConnectorBindingState(String connectorBindingName) throws MetaMatrixComponentException {
        return null;
    }

    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public ConnectorBinding getConnectorBinding(String connectorBindingName) throws MetaMatrixComponentException {
        return null;
    }

    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectorBindingStatistics(java.lang.String)
     * @since 4.3
     */
    public Collection getConnectorBindingStatistics(String connectorBindingName) throws MetaMatrixComponentException {
        return null;
    }
    
    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectionPoolStatistics(java.lang.String)
     * @since 6.1
     */
    public Collection getConnectionPoolStatistics(String connectorBindingName) throws MetaMatrixComponentException {
    	return null;
    } 

    /** 
     * @see com.metamatrix.dqp.service.DataService#clearConnectorBindingCache(java.lang.String)
     * @since 4.3
     */
    public void clearConnectorBindingCache(String connectorBindingName) throws MetaMatrixComponentException {
    }

	public void cancelRequest(AtomicRequestID request, ConnectorID connectorId)
			throws MetaMatrixComponentException {
		
	}

	public void closeRequest(AtomicRequestID request, ConnectorID connectorId)
			throws MetaMatrixComponentException {
		
	}

	public void requestBatch(AtomicRequestID request, ConnectorID connectorId)
			throws MetaMatrixComponentException {
		
	}
	
    @Override
    public ConnectorMetadata getConnectorMetadata(String vdbName,
    		String vdbVersion, String modelName, Properties importProperties) {
    	throw new UnsupportedOperationException();
    }

}
