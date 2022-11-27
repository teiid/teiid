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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.LanguageVisitor;


/**
 * This class represents a compound criteria for logical expressions.  A logical
 * expression involves one or more criteria and a logical operator.  The valid
 * operators are "AND" and "OR".
 */
public class CompoundCriteria extends LogicalCriteria {

    /** Constant indicating the logical "or" of two or more criteria. */
    public static final int OR = 1;

    /** Constant indicating the logical "and" of two or more criteria.*/
    public static final int AND = 0;

    /** The criterias. */
    private List<Criteria> criteria;  // List<Criteria>

    /** The logical operator. */
    private int operator = 0;

    /**
     * Constructs a default instance of this class.
     */
    public CompoundCriteria() {
        criteria = new ArrayList<Criteria>(2);
    }

    /**
     * Constructs an instance of this class given the criteria.  Subclasses are
     * responsible for defining how the criteria are inter-related in a logical
     * expression.
     * @param criteria List of {@link Criteria} being added
     */
    public CompoundCriteria( List<? extends Criteria> criteria ) {
        this();
        Iterator<? extends Criteria> iter = criteria.iterator();
        while ( iter.hasNext() ) {
            addCriteria( iter.next() );
        }
    }

    /**
     * Constructs an instance of this class given a binary logical expression.
     * The logical expression is constructed in terms of a left criteria clause,
     * an operator (either OR or AND), and a right criteria clause.
     *
     * @param left The criteria left of the operator
     * @param right The criteria right of the operator
     * @param operator The logical operator
     *
     * @see #set(int,Criteria,Criteria)
     */
    public CompoundCriteria( int operator, Criteria left, Criteria right ) {
        this();
        set(operator,left,right);
    }


    /**
     * Constructs an instance of this class given a general logical expression.
     * The logical expression is constructed in terms of an operator (either OR
     * or AND), and a set of criteria clauses.
     *
     * @param operator The logical operator
     * @param criteria The list of {@link Criteria}
     */
    public CompoundCriteria( int operator, List criteria ) {
        this();
        set(operator,criteria);
    }

    /**
     * Returns the operator used in the logical expression.  The returned value
     * can be compared to constants defined in this class.
     * @return The operator
     */
    public int getOperator() {
        return this.operator;
    }

    /**
     * Return true if the specified operator is a valid operator
     * @param operator Operator to check
     * @return True if valid
     */
    private boolean isValidOperator(int operator) {
        return (operator == OR || operator == AND);
    }

    /**
     * Sets the operator used in the logical expression.
     * @param operator The operator
     * @throws IllegalArgumentException If operator is invalid
     */
    public void setOperator(int operator) {
        if (!isValidOperator(operator)) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0002", operator)); //$NON-NLS-1$
        }
        this.operator = operator;
    }

    /**
     * Returns the list of criteria.
     * @return List of {@link Criteria}
     */
    public List<Criteria> getCriteria() {
        return this.criteria;
    }

    /**
     * Sets the criteria.
     * @param criteria The list of {@link Criteria}
     */
    public void setCriteria(List<Criteria> criteria) {
        this.criteria = criteria;
    }

    /**
     * Returns the number of criteria in this clause.
     * @return Criteria
     */
    public int getCriteriaCount() {
        return this.criteria.size();
    }

    /**
     * Returns the criteria at the specified index.
     * @return Criteria
     * @throws IllegalArgumentException if no criteria have been specified
     */
    public Criteria getCriteria( int index ) {
        return this.criteria.get(index);
    }

    /**
     * Add another criteria to the clause.
     * @param criteria The criteria
     */
    public void addCriteria( Criteria criteria ) {
        this.criteria.add(criteria);
    }

    /**
     * Reset criteria so there are no more.   After this call, <code>
     * getCriteriaCount</code> will return 0.
     */
    protected void reset() {
        criteria.clear();
    }

    /**
     * Sets a "standard" operand-operator-operand criteria.
     *
     * @param operator The logical operator
     * @param left     The first criteria
     * @param right    The second criteria
     *
     * @see #set(int,List)
     */
    public void set( int operator, Criteria left, Criteria right ) {
        reset();

        setOperator(operator);
        addCriteria(left);
        addCriteria(right);
    }

    /**
     * Sets a "standard" unary criteria.
     *
     * @param operator The unary logical operator
     * @param criteria The criteria
     *
     * @see #set(int,List)
     */
    public void set( int operator, Criteria criteria) {
        reset();
        setOperator(operator);
        addCriteria(criteria);
    }

    /**
     * Sets the operator and an arbitrary set of criteria.
     *
     * @param operator The logical operator
     * @param criteria The set of criteria
     *
     * @see #set(int,Criteria,Criteria)
     */
    public void set( int operator, List criteria ) {
        reset();

        setOperator(operator);

        Iterator iter = criteria.iterator();
        while ( iter.hasNext() ) {
            addCriteria( (Criteria)iter.next() );
        }
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Get hash code.  WARNING: The hash code is based on data in the criteria.
     * If data values are changed, the hash code will change - don't hash this
     * object and change values.
     */
    public int hashCode() {
        int hc = 0;
        hc = HashCodeUtil.hashCode(hc, getOperator());
        hc = HashCodeUtil.hashCode(hc, getCriteria());
        return hc;
    }

    /**
     * Override equals() method.
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if(!(obj instanceof CompoundCriteria)) {
            return false;
        }

        CompoundCriteria cc = (CompoundCriteria)obj;

        return     cc.getOperator() == getOperator() &&
                EquivalenceUtil.areEqual(cc.getCriteria(), getCriteria());
    }

    /**
     * Deep clone.  It returns a new LogicalCriteria with a new list of clones
     * of the criteria objects.
     */
    public Object clone() {
        CompoundCriteria copy = new CompoundCriteria();
        copy.setOperator(getOperator());

        // Clone each sub-criteria
        List crits = getCriteria();
        for(int i=0; i<crits.size(); i++) {
            Criteria crit = (Criteria) crits.get(i);
            if(crit == null) {
                copy.addCriteria(null);
            } else {
                copy.addCriteria( (Criteria) crit.clone() );
            }
        }

        return copy;
    }

}  // END CLASS
