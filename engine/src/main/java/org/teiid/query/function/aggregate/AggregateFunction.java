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

import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.util.CommandContext;


/**
 * This represents the interface for an aggregate function.  The basic lifecycle
 * is that the AggregateFunction is initialize()'d with the type of the element
 * being aggregated, then addInput() is called for every row in the group, then
 * getResult() is called to retrieve the result.
 */
public abstract class AggregateFunction {

    protected int[] argIndexes;
    private int conditionIndex = -1;

    public void setArgIndexes(int[] argIndexes) {
        this.argIndexes = argIndexes;
    }

    public void setConditionIndex(int conditionIndex) {
        this.conditionIndex = conditionIndex;
    }

    /**
     * Called to initialize the function.  In the future this may expand
     * with additional information.
     * @param dataType Data type of element begin aggregated
     * @param inputTypes
     */
    public void initialize(Class<?> dataType, Class<?>[] inputTypes) {}

    public int[] getArgIndexes() {
        return argIndexes;
    }

    /**
     * Called to reset the state of the function.
     */
    public abstract void reset();

    public void addInput(List<?> tuple, CommandContext commandContext) throws TeiidComponentException, TeiidProcessingException {
        if (conditionIndex != -1 && !Boolean.TRUE.equals(tuple.get(conditionIndex))) {
            return;
        }
        if (filter(tuple)) {
            return;
        }
        addInputDirect(tuple, commandContext);
    }

    public boolean filter(List<?> tuple) {
        if (!respectsNull()) {
            for (int i = 0; i < argIndexes.length; i++) {
                if (tuple.get(argIndexes[i]) == null) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean respectsNull() {
        return false;
    }

    /**
     * Called for the element value in every row of a group.
     * @param tuple
     * @param commandContext
     * @throws TeiidProcessingException
     */
    public abstract void addInputDirect(List<?> tuple, CommandContext commandContext) throws TeiidComponentException, TeiidProcessingException;

    /**
     * Called after all values have been processed to get the result.
     * @param commandContext
     * @return Result value
     * @throws TeiidProcessingException
     */
    public abstract Object getResult(CommandContext commandContext)
        throws FunctionExecutionException, ExpressionEvaluationException, TeiidComponentException, TeiidProcessingException;

    public List<? extends Class<?>> getStateTypes() {
        return null;
    }

    public void getState(List<Object> state) {

    }

    public int setState(List<?> state, int index) {
        return 0;
    }

    public Class<?> getOutputType(AggregateSymbol function) {
        return function.getType();
    }

}
