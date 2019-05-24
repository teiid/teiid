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

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.util.CommandContext;

/**
 * Just a simple First/Last_value() implementation
 */
public class FirstLastValue extends SingleArgumentAggregateFunction {

    private Class<?> type;
    private Object value;
    private boolean first;
    private boolean set;

    public void reset() {
        value = null;
        set = false;
    }

    public FirstLastValue(Class<?> type, boolean first) {
        this.type = type;
        this.first = first;
    }

    @Override
    public void addInputDirect(Object input, List<?> tuple,
            CommandContext commandContext) throws TeiidProcessingException,
            TeiidComponentException {
        if (!first) {
            value = input;
        } else if (!set) {
            value = input;
            set = true;
        }
    }

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult(CommandContext)
     */
    public Object getResult(CommandContext commandContext) {
        return value;
    }

    @Override
    public void getState(List<Object> state) {
        state.add(value);
        state.add(set);
    }

    @Override
    public int setState(List<?> state, int index) {
        value = state.get(index);
        set = (Boolean)state.get(index++);
        return index++;
    }

    @Override
    public List<? extends Class<?>> getStateTypes() {
        return Arrays.asList(type, Boolean.class);
    }

    @Override
    public boolean respectsNull() {
        return true;
    }

}
