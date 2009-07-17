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

package com.metamatrix.query.function.aggregate;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.FunctionExecutionException;
import com.metamatrix.common.types.DataTypeManager;

/**
 * Accumulates (per tuple) and calculates the sum of the values 
 * of a column.  The type of the result varies depending on the type
 * of the input {@see AggregateSymbol}
 */
public class Sum implements AggregateFunction {

    // Various possible accumulators, depending on type
    protected static final int LONG = 0;
    protected static final int DOUBLE = 1;
    protected static final int BIG_INTEGER = 2;
    protected static final int BIG_DECIMAL = 3;
    
    protected int accumulatorType = LONG;
    
    private Object sum = null;

    /**
     * Constructor for Sum.
     */
    public Sum() {
    }
    
    /**
     * Allows subclasses to determine type of accumulator for the SUM.
     * @return Type, as defined in constants
     */    
    protected int getAccumulatorType() {
        return this.accumulatorType;
    }

    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#initialize(boolean, String)
     */
    public void initialize(Class dataType, Class inputType) {
        if(dataType.equals(DataTypeManager.DefaultDataClasses.LONG)) {
                    
            this.accumulatorType = LONG;    
            
        } else if(dataType.equals(DataTypeManager.DefaultDataClasses.DOUBLE)) {
        
            this.accumulatorType = DOUBLE;

        } else if(dataType.equals(DataTypeManager.DefaultDataClasses.BIG_INTEGER)) {
        
            this.accumulatorType = BIG_INTEGER;
        } else {
            this.accumulatorType = BIG_DECIMAL;
        }
    }
    
    public void reset() {
        sum = null;
    }

    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#addInput(Object)
     */
    public void addInput(Object input)
        throws FunctionExecutionException, ExpressionEvaluationException, MetaMatrixComponentException {
        
        if (this.sum == null) {
            switch (this.accumulatorType) {
                case LONG:
                    this.sum = new Long(0);
                    break;
                case DOUBLE:
                    this.sum = new Double(0);
                    break;
                case BIG_INTEGER:
                    this.sum = new BigInteger(String.valueOf(0));
                    break;
                case BIG_DECIMAL:
                    this.sum = new BigDecimal(0);
                    break;
            }
        }
            
        switch(this.accumulatorType) {        
            case LONG:
                this.sum = new Long(((Long)this.sum).longValue() + ((Number)input).longValue());
                break;
            case DOUBLE:
                this.sum = new Double(((Double)this.sum).doubleValue() + ((Number)input).doubleValue());
                break;
            case BIG_INTEGER:
                this.sum = ((BigInteger)this.sum).add( (BigInteger) input );
                break;
            case BIG_DECIMAL:
                if (input instanceof BigInteger) {
                    BigInteger bigIntegerInput = (BigInteger) input;
                    this.sum = ((BigDecimal)this.sum).add( new BigDecimal(bigIntegerInput) );
                } else {
                    this.sum = ((BigDecimal)this.sum).add( (BigDecimal) input );
                }
                break;    
        }
    }

    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#getResult()
     */
    public Object getResult() 
        throws FunctionExecutionException, ExpressionEvaluationException, MetaMatrixComponentException {
        
        return sum;        
    }
}
