/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.function.aggregate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.util.CommandContext;


/**
 * Accumulates (per tuple) and calculates the sum of the values
 * of a column.  The type of the result varies depending on the type
 * of the input {@link AggregateSymbol}
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

    @Override
    public void initialize(Class<?> dataType, Class<?> inputType) {
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
        sumLong = 0;
        sumDouble = 0;
        sumBigDecimal = null;
        isNull = true;
    }

    @Override
    public void addInputDirect(Object input, List<?> tuple, CommandContext commandContext)
        throws FunctionExecutionException, ExpressionEvaluationException, TeiidComponentException {

        isNull = false;

        switch(this.accumulatorType) {
            case LONG:
                this.sumLong = FunctionMethods.plus(this.sumLong, ((Number)input).longValue());
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

    @Override
    public void getState(List<Object> state) {
        switch (this.accumulatorType) {
        case LONG:
            if (isNull) {
                state.add(null);
            } else {
                state.add(sumLong);
            }
            break;
        case DOUBLE:
            if (isNull) {
                state.add(null);
            } else {
                state.add(sumDouble);
            }
            break;
        default:
            state.add(sumBigDecimal);
            break;
        }
    }

    @Override
    public List<? extends Class<?>> getStateTypes() {
        switch (this.accumulatorType) {
        case LONG:
            return Arrays.asList(Long.class);
        case DOUBLE:
            return Arrays.asList(Double.class);
        default:
            return Arrays.asList(BigDecimal.class);
        }
    }

    public int setState(java.util.List<?> state, int index) {
        switch (this.accumulatorType) {
        case LONG:
        {
            Long val = (Long)state.get(index);
            if (val == null) {
                isNull = true;
                sumLong = 0;
            } else {
                isNull = false;
                sumLong = val;
            }
            break;
        }
        case DOUBLE:
        {
            Double val = (Double)state.get(index);
            if (val == null) {
                isNull = true;
                sumDouble = 0;
            } else {
                isNull = false;
                sumDouble = val;
            }
            break;
        }
        default:
            this.sumBigDecimal = (BigDecimal)state.get(index);
            if (this.sumBigDecimal != null) {
                isNull = false;
            } else {
                isNull = true;
            }
            break;
        }
        return index + 1;
    }

}
