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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.common.buffer.impl.SizeUtility;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.util.CommandContext;

public class ArrayAgg extends SingleArgumentAggregateFunction {

    private static final int MAX_SIZE = 1 << 23;
    private ArrayList<Object> result;
    private Class<?> componentType;
    private long size;
    private int elemSize;

    @Override
    public void initialize(Class<?> dataType, Class<?> inputType) {
        this.componentType = inputType;
        if (!SizeUtility.isVariableSize(componentType)) {
            elemSize = SizeUtility.getSize(false, componentType);
        }
    }

    @Override
    public void addInputDirect(Object input, List<?> tuple, CommandContext commandContext) throws TeiidComponentException, TeiidProcessingException {
        if (this.result == null) {
            this.result = new ArrayList<Object>();
            size = 0;
        }
        this.result.add(input);
        size += SizeUtility.REFERENCE_SIZE;
        if (input != null) {
            if (elemSize != 0) {
                size += elemSize;
            } else {
                size += SizeUtility.getSize(input, false);
            }
        }
        if (size > MAX_SIZE) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID31209, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31209, MAX_SIZE));
        }
    }

    @Override
    public Object getResult(CommandContext commandContext) throws FunctionExecutionException, ExpressionEvaluationException, TeiidComponentException,TeiidProcessingException {
        if (this.result == null) {
            return null;
        }
        if (this.componentType == DataTypeManager.DefaultDataClasses.OBJECT) {
            return new ArrayImpl(this.result.toArray());
        }
        Object array = Array.newInstance(componentType, this.result.size());
        for (int i = 0; i < result.size(); i++) {
            Object val = result.get(i);
            if (val instanceof ArrayImpl) {
                val = ((ArrayImpl)val).getValues();
            }
            Array.set(array, i, val);
        }
        return new ArrayImpl((Object[]) array);
    }

    @Override
    public void reset() {
        this.result = null;
    }

    @Override
    public boolean respectsNull() {
        return true;
    }

}
