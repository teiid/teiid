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
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.util.CommandContext;


/**
 * Accumulates (per tuple) and calculates the average of the values 
 * of a column.  The type of the result varies depending on the type
 * of the input {@see AggregateSymbol} - the type will not be an
 * integral type but will always be some kind of decimal type.
 */
public class Avg extends Sum {

    private int count = 0;

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#initialize(String, Class)
     */
    public void initialize(Class<?> dataType, Class<?> inputType) {
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
     * @see org.teiid.query.function.aggregate.AggregateFunction#addInputDirect(List, CommandContext, CommandContext)
     */
    public void addInputDirect(Object input, List<?> tuple, CommandContext commandContext)
        throws FunctionExecutionException, ExpressionEvaluationException, TeiidComponentException {

        super.addInputDirect(input, tuple, commandContext);
        count++;
    }

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult(CommandContext)
     */
    public Object getResult(CommandContext commandContext)
        throws FunctionExecutionException, ExpressionEvaluationException, TeiidComponentException {

        Object sum = super.getResult(commandContext);
        if (count == 0 || sum == null) {
            return null;
        }

        switch(getAccumulatorType()) {
            case DOUBLE:
                return new Double( ((Double)sum).doubleValue() / count );

            case BIG_DECIMAL:
                try {
                    return FunctionMethods.divide((BigDecimal)sum, new BigDecimal(count));
                } catch(ArithmeticException e) {
                     throw new FunctionExecutionException(QueryPlugin.Event.TEIID30424, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30424, sum, count));
                }
            default:
                throw new AssertionError("unknown accumulator type"); //$NON-NLS-1$

        }
    }

}
