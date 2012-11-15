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
import java.util.List;
import java.util.Random;

import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.UpdateExecution;


/**
 * Represents the execution of a command.
 */
public class LoopbackExecution implements UpdateExecution, ProcedureExecution {
//     Connector resources
    private LoopbackExecutionFactory config;
    private Command command;
    private String staticStringValue = ""; //$NON-NLS-1$
        
    // Execution state
    private Random randomNumber = new Random();
    private List<Object> row;
    private List<Class<?>> types;
    private boolean waited = false;
    private int rowsReturned = 0;
    private int rowsNeeded = 1;
	private BigInteger rowNumber = BigInteger.ZERO;
    
    public LoopbackExecution(Command command, LoopbackExecutionFactory config) {
        this.config = config;
        this.command = command;
            
        staticStringValue = constructIncrementedString(config.getCharacterValuesSize());
    }
    
    /**
     * Creates string "ABCD...ZABC..." of length characterValueSize
     * @param characterValuesSize
     * @return
     */
    public static String constructIncrementedString(int characterValuesSize) {
        //Create string of length as defined in the translator configuration
    	StringBuffer alphaString = new StringBuffer();
        int genAlphaSize = characterValuesSize;
        char currentChar = 'A';
        while (genAlphaSize-- != 0){
        	alphaString.append(currentChar);
        	if (currentChar =='Z')
        		currentChar = 'A';
        	else
        		currentChar += 1;
        }
        return alphaString.toString();
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
                DataNotAvailableException dnae = new DataNotAvailableException(randomTimeToWait);
                dnae.setStrict(true);
                throw dnae;
            } 
            try {
                Thread.sleep(randomTimeToWait);
            } catch(InterruptedException e) {
            }
            waited = true;
        }
                
        List<Object> resultRow = row;
        if(rowsReturned < this.rowsNeeded && resultRow.size() > 0) {
            rowsReturned++;            
            if (config.getIncrementRows()) {
            	rowNumber = rowNumber.add(BigInteger.ONE);
        		generateRow();
            }
            return resultRow;
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
        determineOutputTypes();
        generateRow();
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

    private void determineOutputTypes() {            
        // Get select columns and lookup the types in metadata
        if(command instanceof QueryExpression) {
            QueryExpression query = (QueryExpression) command;
            types = Arrays.asList(query.getColumnTypes());  
            return;
        }
        if (command instanceof Call) {
        	types = Arrays.asList(((Call)command).getResultSetColumnTypes());
        	return;
        }
        types = new ArrayList<Class<?>>(1);
        types.add(Integer.class);
    }
    
    public static final Long DAY_SECONDS=86400L;
    private static final int DATE_PERIOD = 365*(8099-1970);
    
    /**
	 * Increments value in each column. If the value is bounded type (e.g. short) it will cycle the values.
	 */
	private void generateRow() {
		List<Object> newRow = new ArrayList<Object>(types.size());
		String incrementedString = incrementString(staticStringValue,rowNumber);
		for (Class<?> type : types) {
			if (type.equals(Integer.class)) {
				newRow.add(rowNumber.intValue());
			} else if (type.equals(java.lang.Short.class)) {
				newRow.add(rowNumber.shortValue());
			} else if (type.equals(java.lang.Long.class)) {
				newRow.add(rowNumber.longValue());
			} else if (type.equals(java.lang.Float.class)) {
				newRow.add(rowNumber.floatValue()/10);
			} else if (type.equals(java.lang.Double.class)) {
				newRow.add(rowNumber.doubleValue()/10);
			} else if (type.equals(java.lang.Character.class)) {
				newRow.add((char)((((rowNumber.byteValue()&0xff) + 2)%26) + 97));
			} else if (type.equals(java.lang.Byte.class)) {
				newRow.add(rowNumber.byteValue());
			} else if (type.equals(java.lang.Boolean.class)) {
				newRow.add(rowNumber.intValue()%2!=0);
			} else if (type.equals(java.math.BigInteger.class)) {
				newRow.add(rowNumber);
			} else if (type.equals(java.math.BigDecimal.class)) {
				newRow.add(new BigDecimal(rowNumber, 1));
			} else if (type.equals(java.sql.Date.class)) {
				newRow.add(new Date(DAY_SECONDS * 1000 * (rowNumber.intValue()%DATE_PERIOD)));
			} else if (type.equals(java.sql.Time.class)) {
				newRow.add(new Time((rowNumber.intValue()%DAY_SECONDS) * 1000));
			} else if (type.equals(java.sql.Timestamp.class)) {
				newRow.add(new Timestamp(rowNumber.longValue()));
			} else if (type.equals(TypeFacility.RUNTIME_TYPES.CLOB)) {
				newRow.add(this.config.getTypeFacility().convertToRuntimeType(incrementedString.toCharArray()));
			} else if (type.equals(TypeFacility.RUNTIME_TYPES.BLOB) || type.equals(TypeFacility.RUNTIME_TYPES.VARBINARY)) {
				newRow.add(this.config.getTypeFacility().convertToRuntimeType(incrementedString.getBytes()));
			} else {
				newRow.add(incrementedString);
			}
		}
		row = newRow;
	}
	
	/**
	 * Increments the string by appending unique number in sequence. Preserves string length.
	 * @param number Number in the sequence of strings (which row)
	 * @return
	 */
	public static String incrementString(String string, BigInteger number) {
		String numberString= number.toString();
		if (number.equals(BigInteger.ZERO)) {
			return string;//Backward compatibility for first string
		}
		return string.substring(0,string.length()-numberString.length())+ numberString;
	}

}
