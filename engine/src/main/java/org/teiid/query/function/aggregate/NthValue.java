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
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.util.CommandContext;

public class NthValue extends AggregateFunction {

    private Object value;

    @Override
    public void addInputDirect(List<?> tuple, CommandContext commandContext)
            throws TeiidComponentException, TeiidProcessingException {
        throw new AssertionError();
    }

    public void addInput(List<?> tuple, CommandContext commandContext, long startFrame, long endFrame, TupleBuffer frame)
            throws TeiidComponentException, TeiidProcessingException {
        Integer nthIndex = (Integer)tuple.get(argIndexes[1]);
        if (nthIndex > endFrame || nthIndex < startFrame) {
            this.value = null;
        } else {
            this.value = frame.getBatch(nthIndex).getTuple(nthIndex).get(argIndexes[0]);
        }
        //TODO: the computation of the nth value should be done as needed, not over the whole input set
    }

    @Override
    public boolean respectsNull() {
        return true;
    }

    @Override
    public void reset() {
        value = null;
    }

    @Override
    public Object getResult(CommandContext commandContext)
            throws FunctionExecutionException, ExpressionEvaluationException,
            TeiidComponentException, TeiidProcessingException {
        return value;
    }

}
