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
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.util.CommandContext;

/**
 * Captures the row number and number of tiles for a given row
 * post processing logic uses the row count over the window
 * to calculate the final output
 */
public class Ntile extends SingleArgumentAggregateFunction {

    private int count = 0;
    private int tiles = 0;

    public void reset() {
        count = 0;
        tiles = 0;
    }

    @Override
    public Object getResult(CommandContext commandContext) {
        return new ArrayImpl(count, tiles);
    }

    @Override
    public void addInputDirect(Object input, List<?> tuple,
            CommandContext commandContext)
            throws TeiidProcessingException, TeiidComponentException {
        if (count == Integer.MAX_VALUE) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID31174, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31174));
        }
        count++;
        tiles = (Integer)input;
        if (tiles < 1) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID31279, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31279));
        }
    }

    @Override
    public Class<?> getOutputType(AggregateSymbol function) {
        return DataTypeManager.getArrayType(DataTypeManager.DefaultDataClasses.INTEGER);
    }

}
