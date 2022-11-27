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

package org.teiid.query.sql.lang;

import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.ExistsCriteria.SubqueryHint;
import org.teiid.query.sql.symbol.ContextReference;
import org.teiid.query.sql.symbol.Expression;


/**
 * A criteria which is true is the expression's value is a member in a list
 * of values returned from a subquery.  This criteria can be represented as
 * "&lt;expression&gt; IN (SELECT ...)".
 */
public class SubquerySetCriteria extends AbstractSetCriteria implements SubqueryContainer<QueryCommand>, ContextReference {

    private static AtomicInteger ID = new AtomicInteger();

    private QueryCommand command;
    private SubqueryHint subqueryHint = new SubqueryHint();
    private String id = "$ssc/id" + ID.getAndIncrement(); //$NON-NLS-1$

    /**
     * Constructor for SubquerySetCriteria.
     */
    public SubquerySetCriteria() {
        super();
    }

    public SubquerySetCriteria(Expression expression, QueryCommand subCommand) {
        setExpression(expression);
        setCommand(subCommand);
    }

    public SubqueryHint getSubqueryHint() {
        return subqueryHint;
    }

    public void setSubqueryHint(SubqueryHint subqueryHint) {
        this.subqueryHint = subqueryHint;
    }

    @Override
    public String getContextSymbol() {
        return id;
    }

    /**
     * Set the subquery command (either a SELECT or a procedure execution).
     * @param command Command to execute to get the values for the criteria
     */
    public void setCommand(QueryCommand command) {
        this.command = command;
    }

    /**
     * Get the subquery command used to produce the values for this SetCriteria.
     * @return Command Command to execute
     */
    public QueryCommand getCommand() {
        return this.command;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Get hash code.  WARNING: The hash code is based on data in the criteria.
     * If data values are changed, the hash code will change - don't hash this
     * object and change values.
     * @return Hash code
     */
    public int hashCode() {
        int hc = 0;
        hc = HashCodeUtil.hashCode(hc, getExpression());
        hc = HashCodeUtil.hashCode(hc, getCommand());
        return hc;
    }

    /**
     * Override equals() method.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Use super.equals() to check obvious stuff and variable
        if(obj == this) {
            return true;
        }

        if(! (obj instanceof SubquerySetCriteria)) {
            return false;
        }

        SubquerySetCriteria sc = (SubquerySetCriteria)obj;

        return this.isNegated() == sc.isNegated() &&
                EquivalenceUtil.areEqual(getExpression(), sc.getExpression()) &&
               EquivalenceUtil.areEqual(getCommand(), sc.getCommand()) &&
               this.subqueryHint.equals(sc.getSubqueryHint());
    }

    /**
     * Deep copy of object.  The values Iterator of this object
     * will not be cloned - it will be null in the new object
     * (see #setValueIterator setValueIterator}).
     * @return Deep copy of object
     */
    public SubquerySetCriteria clone() {
        Expression copy = null;
        if(getExpression() != null) {
            copy = (Expression) getExpression().clone();
        }

        QueryCommand copyCommand = null;
        if(getCommand() != null) {
            copyCommand = (QueryCommand) getCommand().clone();
        }

        SubquerySetCriteria criteriaCopy = new SubquerySetCriteria(copy, copyCommand);
        criteriaCopy.setNegated(isNegated());
        criteriaCopy.subqueryHint = this.subqueryHint.clone();
        return criteriaCopy;
    }

}
