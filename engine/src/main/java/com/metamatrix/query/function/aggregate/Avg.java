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

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.FunctionExecutionException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * Accumulates (per tuple) and calculates the average of the values 
 * of a column.  The type of the result varies depending on the type
 * of the input {@see AggregateSymbol} - the type will not be an
 * integral type but will always be some kind of decimal type.
 */
public class Avg extends Sum {

    private static final int AVG_SCALE = 9;

    private int count = 0;

    /**
     * Constructor for Avg.
     */
    public Avg() {
        super();
    }

    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#initialize(String, Class)
     */
    public void initialize(Class dataType, Class inputType) {
        if (dataType.equals(DataTypeManager.DefaultDataClasses.BIG_DECIMAL)) {
            this.accumulatorType = BIG_DECIMAL;
        } else {
            this.accumulatorType = DOUBLE;
        }
    }

    public void reset() {
        super.reset();
        count = 0;
    }

    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#addInput(Object)
     */
    public void addInput(Object input)
        throws FunctionExecutionException, ExpressionEvaluationException, MetaMatrixComponentException {

        super.addInput(input);
        count++;
    }

    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#getResult()
     */
    public Object getResult()
        throws FunctionExecutionException, ExpressionEvaluationException, MetaMatrixComponentException {

        Object sum = super.getResult();
        if (count == 0 || sum == null) {
            return null;
        }

        switch(getAccumulatorType()) {
            case DOUBLE:
                return new Double( ((Double)sum).doubleValue() / count );

            case BIG_DECIMAL:
                try {
                    return ((BigDecimal)sum).divide(new BigDecimal(count), AVG_SCALE, BigDecimal.ROUND_HALF_UP);
                } catch(ArithmeticException e) {
                    throw new FunctionExecutionException(e, ErrorMessageKeys.FUNCTION_0048, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0048, sum, new Integer(count)));
                }
            default:
                throw new AssertionError("unknown accumulator type"); //$NON-NLS-1$

        }
    }

}
