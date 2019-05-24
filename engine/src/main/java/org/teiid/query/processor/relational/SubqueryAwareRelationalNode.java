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

package org.teiid.query.processor.relational;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.TableFunctionReference;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;

public abstract class SubqueryAwareRelationalNode extends RelationalNode {

    private SubqueryAwareEvaluator evaluator;

    protected SubqueryAwareRelationalNode() {
        super();
    }

    public SubqueryAwareRelationalNode(int nodeID) {
        super(nodeID);
    }

    protected Evaluator getEvaluator(Map elementMap) {
        if (this.evaluator == null) {
            this.evaluator = new SubqueryAwareEvaluator(elementMap, getDataManager(), getContext(), getBufferManager());
        } else {
            this.evaluator.initialize(getContext(), getDataManager());
        }
        return this.evaluator;
    }

    @Override
    public void reset() {
        super.reset();
        if (evaluator != null) {
            evaluator.reset();
        }
    }

    @Override
    public void closeDirect() {
        if (evaluator != null) {
            evaluator.close();
        }
    }

    protected void setReferenceValues(TableFunctionReference ref) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        if (ref.getCorrelatedReferences() == null) {
            return;
        }
        for (Map.Entry<ElementSymbol, Expression> entry : ref.getCorrelatedReferences().asMap().entrySet()) {
            getContext().getVariableContext().setValue(entry.getKey(), getEvaluator(Collections.emptyMap()).evaluate(entry.getValue(), null));
        }
    }

    abstract public Collection<? extends LanguageObject> getObjects();

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        List<SubqueryContainer<?>> valueIteratorProviders = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(getObjects());
        return requiresTransaction(transactionalReads, valueIteratorProviders);
    }

    public static Boolean requiresTransaction(boolean transactionalReads,
            List<SubqueryContainer<?>> valueIteratorProviders) {
        for (SubqueryContainer<?> subquery : valueIteratorProviders) {
            ProcessorPlan plan = subquery.getCommand().getProcessorPlan();
            if (plan != null) {
                Boolean txn = plan.requiresTransaction(transactionalReads);
                if (txn == null || txn) {
                    return true; //we can't ensure that this is read only
                }
            }
        }
        return false;
    }

}
