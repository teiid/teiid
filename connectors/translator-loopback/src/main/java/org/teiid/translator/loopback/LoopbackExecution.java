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

package org.teiid.translator.loopback;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Argument.Direction;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.UpdateExecution;


/**
 * Represents the execution of a command.
 */
public class LoopbackExecution implements UpdateExecution, ProcedureExecution {

    private static final String ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"; //$NON-NLS-1$

    // Connector resources
    private LoopbackExecutionFactory config;
    private Command command;
    
    // Execution state
    private Random randomNumber = new Random(System.currentTimeMillis());
    private List<Object> row;
    private boolean waited = false;
    private int rowsReturned = 0;
    private int rowsNeeded = 1;
    
    public LoopbackExecution(Command command, LoopbackExecutionFactory config) {
        this.config = config;
        this.command = command;
    }
    
    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        // Wait on first batch if necessary
        if(this.config.getWaitTime() > 0 && !waited) {
            // Wait a random amount of time up to waitTime milliseconds
            int randomTimeToWait = randomNumber.nextInt(this.config.getWaitTime());

            // If we're asynch and the wait time was longer than the poll interval,
            // then just say we don't have results instead
            if(randomTimeToWait > this.config.getPollIntervalInMilli()) {
            	waited = true;
                throw new DataNotAvailableException(randomTimeToWait);
            } 
            try {
                Thread.sleep(randomTimeToWait);
            } catch(InterruptedException e) {
            }
            waited = true;
        }
                
        if(rowsReturned < this.rowsNeeded && row.size() > 0) {
            rowsReturned++;            
            return row;
        }
        
        return null;
    }

    @Override
    public void execute() throws TranslatorException {
       
    	// Log our command
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Loopback executing command: " + command); //$NON-NLS-1$

        if(this.config.isThrowError()) {
            throw new TranslatorException("Failing because Error=true"); //$NON-NLS-1$
        }
              
        this.rowsNeeded = this.config.getRowCount();
        
        if (command instanceof QueryExpression) {
            QueryExpression queryCommand = (QueryExpression)command;
            if (queryCommand.getLimit() != null) {
            	this.rowsNeeded = queryCommand.getLimit().getRowLimit();
            }
        }
        
        // Prepare for execution
        List types = determineOutputTypes(this.command);
        createDummyRow(types);        
    }
    
    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException,
    		TranslatorException {
    	return new int [] {0};
    }
    
    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
    	Call proc = (Call)this.command;
    	int count = proc.getReturnType() != null ? 1:0;
    	for (Argument param : proc.getArguments()) {
			if (param.getDirection() == Direction.INOUT || param.getDirection() == Direction.OUT) {
				count++;
			}
		}
    	return Arrays.asList(new Object[count]);
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public void cancel() throws TranslatorException {

    }

    private List determineOutputTypes(Command command) {            
        // Get select columns and lookup the types in metadata
        if(command instanceof QueryExpression) {
            QueryExpression query = (QueryExpression) command;
            return Arrays.asList(query.getColumnTypes());
        }
        if (command instanceof Call) {
        	return Arrays.asList(((Call)command).getResultSetColumnTypes());
        }
        List<Class<?>> types = new ArrayList<Class<?>>(1);
        types.add(Integer.class);
        return types;
    }
    
    private void createDummyRow(List<Class<?>> types) {
        row = new ArrayList<Object>(types.size());
        
        for (Class<?> type : types) {
            row.add( getValue(type) );
        }   
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
            return this.config.getTypeFacility().convertToRuntimeType(ALPHA.toCharArray());
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.BLOB)) {
            return this.config.getTypeFacility().convertToRuntimeType(ALPHA.getBytes());
        } else {
            return getVariableString(10);
        }
    }

}
