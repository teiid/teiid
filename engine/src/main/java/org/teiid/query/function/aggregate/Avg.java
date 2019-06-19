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
import java.util.ArrayList;
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.util.CommandContext;


/**
 * Accumulates (per tuple) and calculates the average of the values
 * of a column.  The type of the result varies depending on the type
 * of the input {@link AggregateSymbol} - the type will not be an
 * integral type but will always be some kind of decimal type.
 */
public class Avg extends Sum {

    private long count = 0;

    @Override
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

    @Override
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

    @Override
    public List<? extends Class<?>> getStateTypes() {
        ArrayList<Class<?>> result = new ArrayList<Class<?>>();
        result.addAll(super.getStateTypes());
        result.add(Long.class);
        return result;
    }

    @Override
    public void getState(List<Object> state) {
        super.getState(state);
        state.add(count);
    }

    @Override
    public int setState(List<?> state, int index) {
        index = super.setState(state, index);
        count = (Long) state.get(index);
        return index++;
    }

}
