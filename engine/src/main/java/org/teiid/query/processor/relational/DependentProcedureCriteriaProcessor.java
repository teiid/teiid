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

import java.util.Iterator;
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.util.Assertion;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.util.VariableContext;


public class DependentProcedureCriteriaProcessor extends DependentCriteriaProcessor {

    private List inputReferences;
    private List inputDefaults;
    private Criteria critInProgress;

    public DependentProcedureCriteriaProcessor(RelationalNode dependentNode,
                                               Criteria dependentCriteria,
                                               List references,
                                               List defaults) throws ExpressionEvaluationException, TeiidComponentException {
        super(1, -1, dependentNode, dependentCriteria);
        this.inputDefaults = defaults;
        this.inputReferences = references;
    }

    protected boolean prepareNextCommand(VariableContext context) throws BlockedException,
                                          TeiidComponentException, TeiidProcessingException {

        if (this.critInProgress == null) {
            critInProgress = prepareCriteria();
        }

        for (int j = 0; j < inputReferences.size(); j++) {

            Reference ref = (Reference)inputReferences.get(j);

            context.remove(ref.getExpression());
        }

        if (critInProgress == QueryRewriter.FALSE_CRITERIA) {
            critInProgress = null;
            consumedCriteria();
            return false;
        }

        boolean validRow = true;

        for (Iterator<Criteria> i = Criteria.separateCriteriaByAnd(critInProgress).iterator(); i.hasNext() && validRow;) {
            Criteria crit = i.next();

            Object value = null;
            boolean nullAllowed = false;
            Reference parameter = null;

            if (crit instanceof IsNullCriteria) {
                parameter = (Reference)((IsNullCriteria)crit).getExpression();
                nullAllowed = true;
            } else if (crit instanceof CompareCriteria) {
                CompareCriteria compare = (CompareCriteria)crit;
                value = compare.getRightExpression();
                if (compare.getLeftExpression() instanceof Array) {
                    Array array = (Array)compare.getLeftExpression();
                    if (value instanceof Expression) {
                        value = eval.evaluate((Expression)value, null);
                    }
                    if (value == null) {
                        validRow = false;
                        break;
                    }
                    ArrayImpl valueArray = (ArrayImpl)value;
                    for (int j = 0; j < array.getExpressions().size(); j++) {
                        validRow = setParam(context, valueArray.getValues()[j], nullAllowed, (Reference) array.getExpressions().get(j));
                        if (!validRow) {
                            break;
                        }
                    }
                    continue;
                }
                parameter = (Reference)compare.getLeftExpression();
            } else {
                Assertion.failed("Unknown predicate type " + crit); //$NON-NLS-1$
            }

            validRow = setParam(context, value, nullAllowed, parameter);
        }

        critInProgress = null;
        consumedCriteria();

        if (!validRow) {
            return false;
        }

        for (int j = 0; j < inputReferences.size(); j++) {
            Object defaultValue = inputDefaults.get(j);

            Reference ref = (Reference)inputReferences.get(j);

            if (defaultValue != null && !context.containsVariable(ref.getExpression())) {
                context.setValue(ref.getExpression(), defaultValue);
            }
        }

        return true;
    }

    private boolean setParam(VariableContext context,
            Object value, boolean nullAllowed, Reference parameter)
            throws ExpressionEvaluationException, BlockedException,
            TeiidComponentException {
        if (value instanceof Expression) {
            value = eval.evaluate((Expression)value, null);
        }

        if (value == null && !nullAllowed) {
            return false;
        }

        ElementSymbol parameterSymbol = parameter.getExpression();
        if (context.containsVariable(parameterSymbol)) {
            Object existingValue = context.getValue(parameterSymbol);

            if ((value != null && !value.equals(existingValue)) || (value == null && existingValue != null)) {
                return false;
            }
        }

        context.setValue(parameterSymbol, value);
        return true;
    }

}
