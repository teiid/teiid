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

package org.teiid.query.sql.symbol;

import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.ExistsCriteria.SubqueryHint;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * This is an Expression implementation that can be used in a SELECT clause.
 * It has a subquery Command which must only produce exactly one
 * value (or an Exception will result during query processing). It's type
 * will be the type of the one symbol to be produced. In theory an instance
 * of this could be used wherever an Expression is legal, but it is
 * specifically needed for the SELECT clause.
 */
public class ScalarSubquery implements Expression, SubqueryContainer.Evaluatable<QueryCommand>, ContextReference {

    private static AtomicInteger ID = new AtomicInteger();

    private QueryCommand command;
    private Class<?> type;
    private String id = "$sc/id" + ID.getAndIncrement(); //$NON-NLS-1$
    private boolean shouldEvaluate;

    private SubqueryHint subqueryHint = new SubqueryHint();

    /**
     * Default constructor
     */
    ScalarSubquery() {
        super();
    }

    public ScalarSubquery(QueryCommand subqueryCommand){
        this.setCommand(subqueryCommand);
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

    /**
     * @see org.teiid.query.sql.symbol.Expression#getType()
     */
    public Class<?> getType() {
        if (this.type == null){
            Expression symbol = this.command.getProjectedSymbols().iterator().next();
            this.type = symbol.getType();
        }
        //may still be null if this.command wasn't resolved
        return this.type;
    }

    /**
     * Set type of ScalarSubquery
     * @param type New type
     */
    public void setType(Class<?> type) {
        this.type = type;
    }

    public QueryCommand getCommand() {
        return this.command;
    }

    /**
     * Sets the command.
     */
    public void setCommand(QueryCommand command){
        this.command = command;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Compare this ScalarSubquery to another ScalarSubquery for equality.
     * @param obj Other object
     * @return true if objects are equal
     */
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(! (obj instanceof ScalarSubquery)) {
            return false;
        }
        ScalarSubquery other = (ScalarSubquery) obj;

        return other.getCommand().equals(this.getCommand()) &&
                EquivalenceUtil.areEqual(this.subqueryHint, other.subqueryHint);
    }

    /**
     * Get hashcode for the object
     * @return Hash code
     */
    public int hashCode() {
        if (command != null) {
            return command.hashCode();
        }
        return 0;
    }

    /**
     * Returns a safe clone
     * @see java.lang.Object#clone()
     */
    public Object clone() {
        QueryCommand copyCommand = null;
        if(getCommand() != null) {
            copyCommand = (QueryCommand) getCommand().clone();
        }
        ScalarSubquery clone = new ScalarSubquery(copyCommand);
        //Don't invoke the lazy-loading getType()
        clone.setType(this.type);
        clone.shouldEvaluate = this.shouldEvaluate;
        if (this.subqueryHint != null) {
            clone.subqueryHint = subqueryHint.clone();
        }
        return clone;
    }

    /**
     * Returns string representation of this object.
     * @return String representing the object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    public SubqueryHint getSubqueryHint() {
        return subqueryHint;
    }

    public void setSubqueryHint(SubqueryHint subqueryHint) {
        this.subqueryHint = subqueryHint;
    }

}
