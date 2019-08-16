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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.teiid.common.buffer.TupleSource;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;


/**
 * Represents a SQL Insert statement of the form:
 * "INSERT INTO &lt;group&gt; (&lt;variables&gt;) VALUES &lt;values&gt;".
 */
public class Insert extends ProcedureContainer {

    /** Identifies the group to be udpdated. */
    private GroupSymbol group;

    private List<ElementSymbol> variables = new LinkedList<ElementSymbol>();
    private List<Expression> values = new LinkedList<Expression>();

    private QueryCommand queryExpression;

    private TupleSource tupleSource;
    private Criteria constraint;

    private boolean upsert;

    /**
     * Constructs a default instance of this class.
     */
    public Insert() {
    }

    /**
     * Return type of command.
     * @return TYPE_INSERT
     */
    public int getType() {
        return Command.TYPE_INSERT;
    }

    /**
     * Construct an instance with group, variable list (may be null), and values
     * @param group Group associated with this insert
     * @param variables List of ElementSymbols that represent columns for the values, null implies all columns
     * @param values List of Expression values to be inserted
     */
    public Insert(GroupSymbol group, List<ElementSymbol> variables, List values) {
        this.group = group;
        this.variables = variables;
        this.values = values;
    }

    /**
     * Returns the group being inserted into
     * @return Group being inserted into
     */
    public GroupSymbol getGroup() {
        return group;
    }

    /**
     * Set the group for this insert statement
     * @param group Group to be inserted into
     */
    public void setGroup(GroupSymbol group) {
        this.group = group;
    }

    /**
     * Return an ordered List of variables, may be null if no columns were specified
     * @return List of {@link org.teiid.query.sql.symbol.ElementSymbol}
     */
    public List<ElementSymbol> getVariables() {
        return variables;
    }

    /**
     * Add a variable to end of list
     * @param var Variable to add to the list
     */
    public void addVariable(ElementSymbol var) {
        variables.add(var);
    }

    /**
     * Add a collection of variables to end of list
     * @param vars Variables to add to the list - collection of ElementSymbol
     */
    public void addVariables(Collection<ElementSymbol> vars) {
        variables.addAll(vars);
    }

    /**
     * Returns a list of values to insert
     * to be inserted.
     * @return List of {@link org.teiid.query.sql.symbol.Expression}s
     */
    public List getValues() {
        return this.values;
    }

    /**
     * Sets the values to be inserted.
     * @param values List of {@link org.teiid.query.sql.symbol.Expression}s
     */
    public void setValues(List values) {
        this.values.clear();
        this.values.addAll(values);
    }

    /**
     * Set a collection of variables that replace the existing variables
     * @param vars Variables to be set on this object (ElementSymbols)
     */
    public void setVariables(Collection<ElementSymbol> vars) {
        this.variables.clear();
        this.variables.addAll(vars);
    }

    /**
     * Adds a value to the list of values
     * @param value Expression to be added to the list of values
     */
    public void addValue(Expression value) {
        values.add(value);
    }

    public void setQueryExpression( QueryCommand query ) {
        if (query instanceof Query) {
            Query expr = (Query)query;
            //a single row constructor query is the same as values
            if (expr.isRowConstructor()) {
                this.values.clear();
                this.queryExpression = null;
                for (Expression ex : expr.getSelect().getSymbols()) {
                    addValue(SymbolMap.getExpression(ex));
                }
                if (expr.getOption() != null && this.getOption() == null) {
                    //this isn't ideal, parsing associates the option with values
                    this.setOption(expr.getOption());
                }
                return;
            }
        }
        this.queryExpression = query;
    }

    public QueryCommand getQueryExpression() {
        return this.queryExpression;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Get hashcode for command.  WARNING: This hash code relies on the hash codes of the
     * Group, variables.  If the command changes, it's hash code will change and
     * it can be lost from collections.  Hash code is only valid after command has been
     * completely constructed.
     * @return Hash code for object
     */
    public int hashCode() {
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.group);
        myHash = HashCodeUtil.hashCode(myHash, this.variables);
        return myHash;
    }

    /**
     * Compare two Insert commands for equality.  Will only evaluate to equal if
     * they are IDENTICAL: group is equal, value is equal and variables are equal.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests
        if(!(obj instanceof Insert)) {
            return false;
        }

        Insert other = (Insert) obj;

        return EquivalenceUtil.areEqual(getGroup(), other.getGroup()) &&
               EquivalenceUtil.areEqual(getValues(), other.getValues()) &&
               EquivalenceUtil.areEqual(getVariables(), other.getVariables()) &&
               sameOptionAndHint(other) &&
               EquivalenceUtil.areEqual(getQueryExpression(), other.getQueryExpression()) &&
               this.upsert == other.upsert;

    }

    /**
     * Return a deep copy of this Insert.
     * @return Deep copy of Insert
     */
    public Object clone() {
        GroupSymbol copyGroup = null;
        if(group != null) {
            copyGroup = group.clone();
        }

        List<ElementSymbol> copyVars = LanguageObject.Util.deepClone(getVariables(), ElementSymbol.class);

        List<Expression> copyVals = null;

        if ( getValues() != null) {
            copyVals = LanguageObject.Util.deepClone(getValues(), Expression.class);
        }

        Insert copy = new Insert(copyGroup, copyVars, copyVals);
        if (this.queryExpression != null) {
            copy.setQueryExpression((QueryCommand)this.queryExpression.clone());
        }
        this.copyMetadataState(copy);
        if (this.constraint != null) {
            copy.constraint = (Criteria) this.constraint.clone();
        }
        copy.upsert = this.upsert;
        return copy;
    }

    /**
     * Get the ordered list of all elements returned by this query.  These elements
     * may be ElementSymbols or ExpressionSymbols but in all cases each represents a
     * single column.
     * @return Ordered list of SingleElementSymbol
     */
    public List<Expression> getProjectedSymbols(){
        return Command.getUpdateCommandSymbol();
    }

    /**
     * @see org.teiid.query.sql.lang.Command#areResultsCachable()
     */
    public boolean areResultsCachable() {
        return false;
    }

    public void setTupleSource(TupleSource tupleSource) {
        this.tupleSource = tupleSource;
    }

    public TupleSource getTupleSource() {
        return tupleSource;
    }

    public Criteria getConstraint() {
        return constraint;
    }

    public void setConstraint(Criteria constraint) {
        this.constraint = constraint;
    }

    public boolean isUpsert() {
        return upsert;
    }

    public void setUpsert(boolean merge) {
        this.upsert = merge;
    }

}
