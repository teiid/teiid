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

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.JsonType;
import org.teiid.query.function.JSONFunctionMethods.JSONBuilder;
import org.teiid.query.util.CommandContext;

/**
 * Aggregates Json entries
 */
public class JSONArrayAgg extends SingleArgumentAggregateFunction {

    private JsonType result;
    private JSONBuilder concat;

    public JSONArrayAgg() {
    }

    public void reset() {
        concat = null;
        result = null;
    }

    @Override
    public void addInputDirect(Object input, List<?> tuple, CommandContext commandContext) throws TeiidComponentException, TeiidProcessingException {
        if (concat == null) {
            concat = new JSONBuilder(commandContext.getBufferManager());
            concat.start(true);
        }
        concat.addValue(input);
    }

    /**
     * @throws TeiidProcessingException
     * @throws TeiidComponentException
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult(CommandContext)
     */
    public Object getResult(CommandContext commandContext) throws TeiidComponentException, TeiidProcessingException {
        if (result == null) {
            if (concat == null) {
                return null;
            }
            concat.end(true);
            result = concat.close(commandContext);
            concat = null;
        }
        return result;
    }

}
