/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.query.processor.relational;

import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.sql.lang.AbstractSetCriteria;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.util.ValueIterator;

public class DependentProcedureCriteriaProcessor extends DependentCriteriaProcessor {

    private List inputReferences;
    private List inputDefaults;
    private Criteria critInProgress;
    
    public DependentProcedureCriteriaProcessor(RelationalNode dependentNode,
                                               Criteria dependentCriteria,
                                               List references,
                                               List defaults) {
        super(1, dependentNode, dependentCriteria);
        
        this.inputDefaults = defaults;
        this.inputReferences = references;
    }
    
    public void reset() {
        super.reset();
        critInProgress = null;
    }
    
    /**
     * @throws TupleSourceNotFoundException
     * @see com.metamatrix.query.processor.relational.PlanExecutionNode#prepareNextCommand()
     */
    protected boolean prepareNextCommand() throws BlockedException,
                                          MetaMatrixComponentException, MetaMatrixProcessingException {

        if (this.critInProgress == null) {
            critInProgress = prepareCriteria();
        }

        for (int j = 0; j < inputReferences.size(); j++) {

            Reference ref = (Reference)inputReferences.get(j);

            ref.setData(null, null);
        }

        boolean validRow = true;

        for (Iterator i = Criteria.separateCriteriaByAnd(critInProgress).iterator(); i.hasNext();) {
            Criteria crit = (Criteria)i.next();

            Object value = null;
            boolean nullAllowed = false;
            Reference parameter = null;

            if (crit instanceof AbstractSetCriteria) {
                AbstractSetCriteria asc = (AbstractSetCriteria)crit;
                ValueIterator iter = asc.getValueIterator();
                if (iter.hasNext()) {
                    value = asc.getValueIterator().next();
                }
                parameter = (Reference)asc.getExpression();
            } else if (crit instanceof IsNullCriteria) {
                parameter = (Reference)((IsNullCriteria)crit).getExpression();
                nullAllowed = true;
            } else if (crit instanceof CompareCriteria) {
                CompareCriteria compare = (CompareCriteria)crit;
                value = compare.getRightExpression();
                parameter = (Reference)compare.getLeftExpression();
            } else {
                Assertion.failed("Unknown predicate type"); //$NON-NLS-1$
            }

            if (value instanceof Expression) {
                value = Evaluator.evaluate((Expression)value);
            }

            if (value == null && !nullAllowed) {
                validRow = false;
                break;
            }

            if (parameter.getTuple() != null) {
                Object existingValue = parameter.getTuple().get(0);

                if ((value != null && !value.equals(existingValue)) || (value == null && existingValue != null)) {
                    validRow = false;
                    break;
                }
            }

            parameter.setValue(value);
        }

        critInProgress = null;
        consumedCriteria();

        if (!validRow) {
            return false;
        }

        for (int j = 0; j < inputReferences.size(); j++) {
            Object defaultValue = inputDefaults.get(j);

            Reference ref = (Reference)inputReferences.get(j);

            if (defaultValue != null && ref.getTuple() == null) {
                ref.setValue(defaultValue);
            }
            
            Assertion.isNotNull(ref.getTuple());
        }

        return true;
    }

}
