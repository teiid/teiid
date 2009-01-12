/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.loopback;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.metamatrix.data.api.AsynchQueryCommandExecution;
import com.metamatrix.data.api.AsynchQueryExecution;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ProcedureExecution;
import com.metamatrix.data.api.SynchQueryCommandExecution;
import com.metamatrix.data.api.SynchQueryExecution;
import com.metamatrix.data.api.TypeFacility;
import com.metamatrix.data.api.UpdateExecution;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IBulkInsert;
import com.metamatrix.data.language.ICommand;
import com.metamatrix.data.language.IParameter;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.language.IQueryCommand;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * Represents the execution of a command.
 */
public class LoopbackExecution implements SynchQueryCommandExecution, AsynchQueryCommandExecution, UpdateExecution, ProcedureExecution, SynchQueryExecution, AsynchQueryExecution {

    private static final String ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"; //$NON-NLS-1$

    // Connector resources
    private ConnectorEnvironment env;
//    private RuntimeMetadata metadata;
    private ICommand command;
    private int maxBatchSize;

    
    // Configuration
    private int rowsNeeded = 1;
    private int waitTime = 0;
    private boolean error = false;
    private int pollInterval = 1000;
        
    // Execution state
    private Random randomNumber = new Random(System.currentTimeMillis());
    private List row;
    private boolean waited = false;
    private int rowsReturned = 0;
    private boolean asynch = false;

    /**
     * 
     */
    public LoopbackExecution(ConnectorEnvironment env, RuntimeMetadata metadata) {
        this.env = env;
//        this.metadata = metadata;
    }

    /** 
     * @see com.metamatrix.data.api.AsynchQueryExecution#getPollInterval()
     * @since 4.3
     */
    public long getPollInterval() {
        return this.pollInterval;
    }

    /* 
     * @see com.metamatrix.data.SynchExecution#nextBatch(int)
     */
    public Batch nextBatch() throws ConnectorException {
        // Wait on first batch if necessary
        if(waitTime > 0 && !waited) {
            // Wait a random amount of time up to waitTime milliseconds
            int randomTimeToWait = randomNumber.nextInt(waitTime);

            if(asynch) {
                // If we're asynch and the wait time was longer than the poll interval,
                // then just say we don't have results instead
                if(randomTimeToWait > pollInterval) {
                    //System.out.println(System.currentTimeMillis() + ": nextBatch(): no data yet, returning empty batch");                
                    return new BasicBatch(Collections.EMPTY_LIST);
                } // else return results as normal
            } else {    
                try {
                    Thread.sleep(randomTimeToWait);
                } catch(InterruptedException e) {
                }
                waited = true;
            }
        }
                
        BasicBatch batch = new BasicBatch();
        while(rowsReturned < rowsNeeded && batch.getRowCount() < maxBatchSize) {
            batch.addRow(row);
            rowsReturned++;            
        }
        
        if(rowsReturned >= rowsNeeded) {
            batch.setLast();
        }
        
        //System.out.println(System.currentTimeMillis() + ": nextBatch(): returning batch with " + batch.getRowCount() + " rows");                
        return batch;
    }

    /* 
     * @see com.metamatrix.data.SynchQueryExecution#execute(com.metamatrix.data.language.IQuery, int)
     */
    public void execute(IQueryCommand query, int maxBatchSize) throws ConnectorException {
        this.command = query;
        this.maxBatchSize = maxBatchSize;

        // Log our command
        env.getLogger().logTrace("Loopback executing command: " + query); //$NON-NLS-1$

        // Get error mode
        String errorString = env.getProperties().getProperty(LoopbackProperties.ERROR, "false"); //$NON-NLS-1$
        error = errorString.equalsIgnoreCase("true"); //$NON-NLS-1$
        if(error) {
            throw new ConnectorException("Failing because Error=true"); //$NON-NLS-1$
        }
               
        // Get max wait time
        String waitTimeString = env.getProperties().getProperty(LoopbackProperties.WAIT_TIME, "0");               //$NON-NLS-1$
        try {
            waitTime = Integer.parseInt(waitTimeString);
        } catch (Exception e) {
            throw new ConnectorException("Invalid " + LoopbackProperties.WAIT_TIME + "=" + waitTimeString); //$NON-NLS-1$ //$NON-NLS-2$
        }        
        
        // Get # of rows of data to return
        String rowCountString = env.getProperties().getProperty(LoopbackProperties.ROW_COUNT, "1");              //$NON-NLS-1$
        try {
            rowsNeeded = Integer.parseInt(rowCountString);
        } catch (Exception e) {
            throw new ConnectorException("Invalid " + LoopbackProperties.ROW_COUNT + "=" + rowCountString); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if (query.getLimit() != null && query.getLimit().getRowLimit() > 0 && query.getLimit().getRowLimit() < rowsNeeded) {
            rowsNeeded = query.getLimit().getRowLimit();
        }
        
        // Prepare for execution
        List types = determineOutputTypes(this.command);
        this.row = createDummyRow(types);        
    }

    /* 
     * @see com.metamatrix.data.UpdateExecution#execute(com.metamatrix.data.language.ICommand)
     */
    public int execute(ICommand command) throws ConnectorException {
        // Log our command
        env.getLogger().logTrace("Loopback executing command: " + command); //$NON-NLS-1$
                
        return 0;  
    }

    /**
     * @see com.metamatrix.data.api.UpdateExecution#execute(com.metamatrix.data.language.IBulkInsert)
     */
    public int execute(IBulkInsert command) throws ConnectorException {
        env.getLogger().logTrace("Loopback executing command: " + command); //$NON-NLS-1$
        return 0;
    }
    
    /* 
     * @see com.metamatrix.data.ProcedureExecution#execute(com.metamatrix.data.language.IExecute)
     */
    public void execute(IProcedure procedure, int maxBatchSize) throws ConnectorException {
        this.command = procedure;
        this.maxBatchSize = maxBatchSize;

        // Log our command
        env.getLogger().logTrace("Loopback executing command: " + procedure); //$NON-NLS-1$
        
        // Prepare for execution
        List types = determineOutputTypes(this.command);
        this.row = createDummyRow(types);        
    }

    /** 
     * @see com.metamatrix.data.api.AsynchQueryExecution#executeAsynch(com.metamatrix.data.language.IQuery, int)
     * @since 4.3
     */
    public void executeAsynch(IQueryCommand query,
                              int maxBatchSize) throws ConnectorException {

        this.asynch = true;
        execute(query, maxBatchSize);
        
        // Get poll interval
        String pollIntervalString = env.getProperties().getProperty(LoopbackProperties.POLL_INTERVAL, "1000");              //$NON-NLS-1$
        try {
            pollInterval = Integer.parseInt(pollIntervalString);
        } catch (Exception e) {
            throw new ConnectorException("Invalid " + LoopbackProperties.POLL_INTERVAL + "=" + pollIntervalString); //$NON-NLS-1$ //$NON-NLS-2$
        }       
    }

    /* 
     * @see com.metamatrix.data.ProcedureExecution#getOutputValue(com.metamatrix.data.language.IParameter)
     */
    public Object getOutputValue(IParameter parameter) throws ConnectorException {
        return null;
    }
        
    /* 
     * @see com.metamatrix.data.Execution#close()
     */
    public void close() throws ConnectorException {
        // nothing to do
    }

    /* 
     * @see com.metamatrix.data.Execution#cancel()
     */
    public void cancel() throws ConnectorException {

    }

    private List determineOutputTypes(ICommand command) throws ConnectorException {            
        

        // Get select columns and lookup the types in metadata
        if(command instanceof IQueryCommand) {
            IQueryCommand query = (IQueryCommand) command;
            return Arrays.asList(query.getColumnTypes());
        }
        List types = new ArrayList();
        types.add(Integer.class);
        return types;
    }
    
    private List createDummyRow(List types) throws ConnectorException {
        List row = new ArrayList(types.size());
        
        Iterator iter = types.iterator();
        while(iter.hasNext()) {
            Class type = (Class) iter.next();
            row.add( getValue(type) );
        }   
        
        return row;
    }

    /**
     * Get a variable-sized string.
     * @param size The size (in characters) of the string
     */
    private String getVariableString( int size ) {
        int multiplier = size / ALPHA.length();
        int remainder  = size % ALPHA.length();
        String value = ""; //$NON-NLS-1$
        for ( int k = 0; k < multiplier; k++ ) {
            value += ALPHA;
        }
        value += ALPHA.substring(0,remainder);
        return value;
    }
    
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
    public static java.sql.Date SQL_DATE_VAL;
    public static java.sql.Time TIME_VAL;
    public static java.sql.Timestamp TIMESTAMP_VAL;
    
    /*
     * Set the date/time fields to UTC 0 as displayed in US/Central
     */
    static {
    	Calendar cal = Calendar.getInstance();
    	cal.clear();
    	cal.set(1969, 11, 31, 18, 0, 0);
    	SQL_DATE_VAL = new Date(cal.getTimeInMillis());
    	TIME_VAL = new Time(cal.getTimeInMillis());
    	TIMESTAMP_VAL = new Timestamp(cal.getTimeInMillis());
    }
    
    private Object getValue(Class type) {
        if(type.equals(java.lang.String.class)) {
            return getVariableString(10);
        } else if(type.equals(java.lang.Integer.class)) {
            return INTEGER_VAL;
        } else if(type.equals(java.lang.Short.class)) { 
            return SHORT_VAL;    
        } else if(type.equals(java.lang.Long.class)) {
            return LONG_VAL;
        } else if(type.equals(java.lang.Float.class)) {
            return FLOAT_VAL;
        } else if(type.equals(java.lang.Double.class)) {
            return DOUBLE_VAL;
        } else if(type.equals(java.lang.Character.class)) {
            return CHAR_VAL;
        } else if(type.equals(java.lang.Byte.class)) {
            return BYTE_VAL;
        } else if(type.equals(java.lang.Boolean.class)) {
            return BOOLEAN_VAL;
        } else if(type.equals(java.math.BigInteger.class)) {
            return BIG_INTEGER_VAL;
        } else if(type.equals(java.math.BigDecimal.class)) {
            return BIG_DECIMAL_VAL;
        } else if(type.equals(java.sql.Date.class)) {
            return SQL_DATE_VAL;
        } else if(type.equals(java.sql.Time.class)) {
            return TIME_VAL;
        } else if(type.equals(java.sql.Timestamp.class)) {
            return TIMESTAMP_VAL;
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.CLOB)) {
            return env.getTypeFacility().convertToRuntimeType(ALPHA.toCharArray());
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.BLOB)) {
            return env.getTypeFacility().convertToRuntimeType(ALPHA.getBytes());
        } else {
            return getVariableString(10);
        }
    }

	public void execute(IQuery query, int maxBatchSize)
			throws ConnectorException {
		this.execute((IQueryCommand)query, maxBatchSize);
	}

	public void executeAsynch(IQuery query, int maxBatchSize)
			throws ConnectorException {
		this.executeAsynch((IQueryCommand)query, maxBatchSize);
	}

}
