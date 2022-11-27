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
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.util.CommandContext;


/**
 */
public class Min extends SingleArgumentAggregateFunction {

    private Object minValue;
    private Class<?> outputType;

    public void reset() {
        minValue = null;
    }

    @Override
    public void initialize(Class<?> dataType, Class<?> inputType) {
        this.outputType = inputType;
    }

    @Override
    public void addInputDirect(Object value, List<?> tuple, CommandContext commandContext)
        throws FunctionExecutionException, ExpressionEvaluationException, TeiidComponentException {

        if(minValue == null) {
            minValue = value;
        } else {
            Comparable valueComp = (Comparable) value;

            if(Constant.COMPARATOR.compare(valueComp, minValue) < 0) {
                minValue = valueComp;
            }
        }
    }

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult(CommandContext)
     */
    public Object getResult(CommandContext commandContext) {
        return this.minValue;
    }

    @Override
    public List<? extends Class<?>> getStateTypes() {
        return Arrays.asList(outputType);
    }

    @Override
    public void getState(List<Object> state) {
        state.add(minValue);
    }

    @Override
    public int setState(List<?> state, int index) {
        this.minValue = state.get(index);
        return index++;
    }

}
