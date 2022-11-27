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

import java.util.Arrays;
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.util.CommandContext;

public class StatsFunction extends SingleArgumentAggregateFunction {

    private double sum = 0;
    private double sumSq = 0;
    private long count = 0;
    private Type type;

    public StatsFunction(Type function) {
        this.type = function;
    }

    @Override
    public void reset() {
        sum = 0;
        sumSq = 0;
        count = 0;
    }

    @Override
    public void addInputDirect(Object input, List<?> tuple, CommandContext commandContext)
            throws FunctionExecutionException, ExpressionEvaluationException,
            TeiidComponentException {
        sum += ((Number)input).doubleValue();
        sumSq += Math.pow(((Number)input).doubleValue(), 2);
        count++;
    }

    @Override
    public Object getResult(CommandContext commandContext) throws FunctionExecutionException,
            ExpressionEvaluationException, TeiidComponentException {
        double result = 0;
        switch (type) {
        case STDDEV_POP:
        case VAR_POP:
            if (count == 0) {
                return null;
            }
            result = (sumSq - sum * sum / count) / count;
            if (type == Type.STDDEV_POP) {
                result = Math.sqrt(result);
            }
            break;
        case STDDEV_SAMP:
        case VAR_SAMP:
            if (count < 2) {
                return null;
            }
            result = (sumSq - sum * sum / count) / (count - 1);
            if (type == Type.STDDEV_SAMP) {
                result = Math.sqrt(result);
            }
            break;
        }
        return result;
    }

    @Override
    public void getState(List<Object> state) {
        state.add(count);
        state.add(sum);
        state.add(sumSq);
    }

    @Override
    public List<? extends Class<?>> getStateTypes() {
        return Arrays.asList(Long.class, Double.class, Double.class);
    }

    @Override
    public int setState(List<?> state, int index) {
        count = (Long) state.get(index++);
        sum = (Double) state.get(index++);
        sumSq = (Double) state.get(index++);
        return index;
    }

}
