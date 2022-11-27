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
import org.teiid.query.sql.lang.PredicateCriteria.Negatable;
import org.teiid.query.sql.symbol.ContextReference;


/**
 * This predicate criteria implements the "exists" predicate, which has
 * a subquery in it.  For example,
 * "EXISTS (Select EmployeeID FROM Employees WHERE EmployeeName = 'Smith')".
 */
public class ExistsCriteria extends PredicateCriteria
implements SubqueryContainer.Evaluatable<QueryCommand>, ContextReference, Negatable {

    public static class SubqueryHint {
        public static String MJ = "MJ"; //$NON-NLS-1$
        public static String NOUNNEST = "NO_UNNEST"; //$NON-NLS-1$
        public static String DJ = "DJ"; //$NON-NLS-1$

        private boolean mergeJoin;
        private boolean noUnnest;
        private boolean depJoin;

        public void setMergeJoin(boolean semiJoin) {
            this.mergeJoin = semiJoin;
        }

        public boolean isMergeJoin() {
            return mergeJoin;
        }

        public void setNoUnnest(boolean noUnnest) {
            this.noUnnest = noUnnest;
        }

        public boolean isNoUnnest() {
            return noUnnest;
        }

        public void setDepJoin() {
            this.depJoin = true;
            this.mergeJoin = true;
        }

        public boolean isDepJoin() {
            return depJoin;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SubqueryHint)) {
                return false;
            }
            SubqueryHint other = (SubqueryHint) obj;
            return mergeJoin == other.mergeJoin
            && noUnnest == other.noUnnest
            && depJoin == other.depJoin;
        }

        public SubqueryHint clone() {
            SubqueryHint clone = new SubqueryHint();
            clone.mergeJoin = this.mergeJoin;
            clone.noUnnest = this.noUnnest;
            clone.depJoin = this.depJoin;
            return clone;
        }

    }

    private static AtomicInteger ID = new AtomicInteger();

    private QueryCommand command;
    private String id = "$ec/id" + ID.getAndIncrement(); //$NON-NLS-1$
    private boolean shouldEvaluate;
    private boolean negated;
    private SubqueryHint subqueryHint = new SubqueryHint();

    /**
     * Default constructor
     */
    public ExistsCriteria() {
        super();
    }

    public ExistsCriteria(QueryCommand subqueryCommand){
        this.command = subqueryCommand;
    }

    public boolean shouldEvaluate() {
        return shouldEvaluate;
    }

    public void setShouldEvaluate(boolean shouldEvaluate) {
        this.shouldEvaluate = shouldEvaluate;
    }

    @Override
    public String getContextSymbol() {
        return id;
    }

    public QueryCommand getCommand() {
        return this.command;
    }

    public void setCommand(QueryCommand subqueryCommand){
        this.command = subqueryCommand;
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
        hc = HashCodeUtil.hashCode(hc, getCommand());
        return hc;
    }

    /**
     * Override equals() method.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if(!(obj instanceof ExistsCriteria)) {
            return false;
        }

        ExistsCriteria other = (ExistsCriteria)obj;

        return EquivalenceUtil.areEqual(getCommand(), other.getCommand()) &&
            this.negated == other.negated &&
            this.subqueryHint.equals(other.subqueryHint);
    }

    public SubqueryHint getSubqueryHint() {
        return subqueryHint;
    }

    public void setSubqueryHint(SubqueryHint subqueryHint) {
        this.subqueryHint = subqueryHint;
    }

    /**
     * Deep copy of object.  The values Iterator of this object
     * will not be cloned - it will be null in the new object
     * (see #setValueIterator setValueIterator}).
     * @return Deep copy of object
     * @see java.lang.Object#clone()
     */
    public Object clone() {
        ExistsCriteria ec = new ExistsCriteria((QueryCommand) this.command.clone());
        ec.subqueryHint = this.subqueryHint.clone();
        ec.setNegated(this.negated);
        ec.shouldEvaluate = this.shouldEvaluate;
        return ec;
    }

    public boolean isNegated() {
        return negated;
    }

    public void setNegated(boolean negated) {
        this.negated = negated;
    }

    @Override
    public void negate() {
        this.negated = !this.negated;
    }
}
