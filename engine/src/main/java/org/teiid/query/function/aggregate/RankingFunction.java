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
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.util.CommandContext;

/**
 * computes rank/dense_rank
 */
public class RankingFunction extends AggregateFunction {

    private int count = 0;
    private int lastCount = 0;
    private Type type;

    public RankingFunction(Type function) {
        this.type = function;
    }

    @Override
    public void reset() {
        count = 0;
        lastCount = 0;
    }

    @Override
    public void addInputDirect(List<?> tuple, CommandContext commandContext)
            throws FunctionExecutionException, ExpressionEvaluationException,
            TeiidComponentException {
        if (type == Type.RANK) {
            if (count == Integer.MAX_VALUE) {
                throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID31174, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31174));
            }
            count++;
        }
    }

    @Override
    public Object getResult(CommandContext commandContext) throws FunctionExecutionException,
            ExpressionEvaluationException, TeiidComponentException {
        if (type == Type.DENSE_RANK) {
            if (count == Integer.MAX_VALUE) {
                throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID31174, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31174));
            }
            count++;
            return count;
        }
        int result = ++lastCount;
        lastCount = count;
        return result;
    }

}
