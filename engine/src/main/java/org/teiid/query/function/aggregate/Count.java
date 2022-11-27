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
import org.teiid.query.QueryPlugin;
import org.teiid.query.util.CommandContext;

/**
 * Just a simple COUNT() implementation that counts every non-null row it sees.
 */
public class Count extends AggregateFunction {

    private int count = 0;

    public void reset() {
        count = 0;
    }

    @Override
    public void addInputDirect(List<?> tuple, CommandContext commandContext)
            throws TeiidComponentException, TeiidProcessingException {
        if (count == Integer.MAX_VALUE) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID31174, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31174));
        }
        count++;
    }

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult(CommandContext)
     */
    public Object getResult(CommandContext commandContext) {
        return Integer.valueOf(count);
    }

    @Override
    public void getState(List<Object> state) {
        state.add(count);
    }

    @Override
    public int setState(List<?> state, int index) {
        count = (Integer) state.get(index);
        return index++;
    }

    @Override
    public List<? extends Class<?>> getStateTypes() {
        return Arrays.asList(Integer.class);
    }

}
