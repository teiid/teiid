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

import org.teiid.UserDefinedAggregate;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.util.CommandContext;

public class UserDefined extends AggregateFunction {

    private FunctionDescriptor fd;
    private UserDefinedAggregate<?> instance;
    private ExposedStateUserDefinedAggregate<?> exposed;
    private Object[] values;

    public UserDefined(FunctionDescriptor functionDescriptor) throws FunctionExecutionException {
        this.fd = functionDescriptor;
        this.instance = (UserDefinedAggregate<?>) fd.newInstance();
        if (instance instanceof ExposedStateUserDefinedAggregate) {
            this.exposed = (ExposedStateUserDefinedAggregate)instance;
        }
    }

    @Override
    public void addInputDirect(List<?> tuple, CommandContext commandContext) throws TeiidComponentException,
            TeiidProcessingException {
        if (values == null) {
            values = new Object[argIndexes.length + (fd.requiresContext()?1:0)];
        }
        if (fd.requiresContext()) {
            values[0] = commandContext;
        }
        for (int i = 0; i < argIndexes.length; i++) {
            values[i + (fd.requiresContext()?1:0)] = tuple.get(argIndexes[i]);
        }
        fd.invokeFunction(values, commandContext, instance);
    }

    @Override
    public void reset() {
        instance.reset();
    }

    @Override
    public Object getResult(CommandContext commandContext) throws FunctionExecutionException,
            ExpressionEvaluationException, TeiidComponentException,
            TeiidProcessingException {
        return instance.getResult(commandContext);
    }

    @Override
    public boolean respectsNull() {
        return !fd.getMethod().isNullOnNull();
    }

    public List<? extends Class<?>> getStateTypes() {
        if (this.exposed != null) {
            return this.exposed.getStateTypes();
        }
        return null;
    }

    public void getState(List<Object> state) {
        if (this.exposed != null) {
            this.exposed.getState(state);
        }
    }

    public int setState(List<?> state, int index) {
        if (this.exposed != null) {
            return this.exposed.setState(state, index);
        }
        return 0;
    }

}
