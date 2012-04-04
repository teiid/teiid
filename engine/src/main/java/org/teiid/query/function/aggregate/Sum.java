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

package org.teiid.query.function.aggregate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.util.CommandContext;


/**
 * Accumulates (per tuple) and calculates the sum of the values 
 * of a column.  The type of the result varies depending on the type
 * of the input {@see AggregateSymbol}
 */
public class Sum extends SingleArgumentAggregateFunction {

    // Various possible accumulators, depending on type
    protected static final int LONG = 0;
    protected static final int DOUBLE = 1;
    protected static final int BIG_INTEGER = 2;
    protected static final int BIG_DECIMAL = 3;
    
    protected int accumulatorType = LONG;
    
    private long sumLong;
    private double sumDouble;
    private BigDecimal sumBigDecimal;
    private boolean isNull = true;

    /**
     * Allows subclasses to determine type of accumulator for the SUM.
     * @return Type, as defined in constants
     */    
    protected int getAccumulatorType() {
        return this.accumulatorType;
    }

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#initialize(boolean, String)
     */
    public void initialize(Class<?> dataType, Class<?> inputType) {
        if(dataType.equals(DataTypeManager.DefaultDataClasses.LONG)) {
                    
            this.accumulatorType = LONG;    
            
        } else if(dataType.equals(DataTypeManager.DefaultDataClasses.DOUBLE)) {
        
            this.accumulatorType = DOUBLE;

        } else {
            this.accumulatorType = BIG_DECIMAL;
        }
    }
    
    public void reset() {
        sumLong = 0;
        sumDouble = 0;
        sumBigDecimal = null;
        isNull = true;
    }

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#addInputDirect(List, CommandContext, CommandContext)
     */
    public void addInputDirect(Object input, List<?> tuple, CommandContext commandContext)
        throws FunctionExecutionException, ExpressionEvaluationException, TeiidComponentException {
        
    	isNull = false;
    	
        switch(this.accumulatorType) {        
            case LONG:
                this.sumLong += ((Number)input).longValue();
                break;
            case DOUBLE:
                this.sumDouble += ((Number)input).doubleValue();
                break;
            case BIG_INTEGER:
            case BIG_DECIMAL:
            	if (sumBigDecimal == null) {
            		sumBigDecimal = BigDecimal.valueOf(0);
            	}
                if (input instanceof BigInteger) {
                    BigInteger bigIntegerInput = (BigInteger) input;
                    this.sumBigDecimal = this.sumBigDecimal.add( new BigDecimal(bigIntegerInput) );
                } else if (input instanceof BigDecimal){
                    this.sumBigDecimal = this.sumBigDecimal.add( (BigDecimal) input );
                } else {
                	this.sumBigDecimal = this.sumBigDecimal.add( new BigDecimal(((Number)input).longValue()));
                }
                break;    
        }
    }

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult(CommandContext)
     */
    public Object getResult(CommandContext commandContext) 
        throws FunctionExecutionException, ExpressionEvaluationException, TeiidComponentException {
        
    	if (isNull){
    		return null;
    	}

        switch(this.accumulatorType) {        
        case LONG:
        	return this.sumLong;
        case DOUBLE:
            return this.sumDouble;
        case BIG_INTEGER:
        	return this.sumBigDecimal.toBigInteger();
        }
        return this.sumBigDecimal;
    }
}
