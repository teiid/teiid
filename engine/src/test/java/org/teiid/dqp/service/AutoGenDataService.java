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

package org.teiid.dqp.service;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorWork;
import org.teiid.dqp.internal.datamgr.ConnectorWorkItem;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.AtomicResultsMessage;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;


/**
 * This data service will automatically generate results when called with a query - basically
 * the same as the old loopback connector.
 */
public class AutoGenDataService extends ConnectorManager{

    // Number of rows that will be generated for each query
    private int rows = 10;
    private SourceCapabilities caps;
	public boolean throwExceptionOnExecute;
	public int dataNotAvailable = -1;
    
    public AutoGenDataService() {
    	super("FakeConnector","FakeConnector"); //$NON-NLS-1$ //$NON-NLS-2$
        caps = TestOptimizer.getTypicalCapabilities();
    }
    
    public void setCaps(SourceCapabilities caps) {
		this.caps = caps;
	}

    public void setRows(int rows) {
        this.rows = rows;
    }
    
    public int getRows() {
        return this.rows;
    }

    @Override
    public ConnectorWork registerRequest(AtomicRequestMessage message)
    		throws TeiidComponentException {
        List projectedSymbols = (message.getCommand()).getProjectedSymbols();               
        List[] results = createResults(projectedSymbols);
                
        final AtomicResultsMessage msg = ConnectorWorkItem.createResultsMessage(results, projectedSymbols);
        msg.setFinalRow(rows);
        return new ConnectorWork() {
			
			@Override
			public AtomicResultsMessage more() throws TranslatorException {
				throw new RuntimeException("Should not be called"); //$NON-NLS-1$
			}
			
			@Override
			public AtomicResultsMessage execute() throws TranslatorException {
				if (throwExceptionOnExecute) {
		    		throw new TranslatorException("Connector Exception"); //$NON-NLS-1$
		    	}
				if (dataNotAvailable > -1) {
					int delay = dataNotAvailable;
					dataNotAvailable = -1;
					throw new DataNotAvailableException(delay);
				}
				return msg;
			}
			
			@Override
			public void close() {
				
			}
			
			@Override
			public void cancel() {
				
			}
			
		};
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
            rows[i] = row;
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

    @Override
	public SourceCapabilities getCapabilities(){
        return caps;
    }

}
