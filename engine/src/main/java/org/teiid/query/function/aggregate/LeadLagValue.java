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
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.util.CommandContext;

/**
 * We store up to three values related to the lead/lag per row
 */
public class LeadLagValue extends AggregateFunction {

    private Object[] vals = null;
    private int partition = 0;

    @Override
    public void addInputDirect(List<?> tuple, CommandContext commandContext)
            throws TeiidComponentException, TeiidProcessingException {
        vals = new Object[argIndexes.length + 1];
        for (int i = 0; i < argIndexes.length; i++) {
            vals[i] = tuple.get(argIndexes[i]);
        }
        vals[argIndexes.length] = partition;
    }

    @Override
    public Object getResult(CommandContext commandContext)
            throws FunctionExecutionException, ExpressionEvaluationException,
            TeiidComponentException, TeiidProcessingException {
        return new ArrayImpl(vals);
    }

    @Override
    public void reset() {
        vals = null;
        partition++;
    }

    @Override
    public boolean respectsNull() {
        return true;
    }

    @Override
    public Class<?> getOutputType(AggregateSymbol function) {
        return DataTypeManager.getArrayType(DataTypeManager.DefaultDataClasses.OBJECT);
    }

}
