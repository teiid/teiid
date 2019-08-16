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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;


/**
 * A criteria which is true is the expression's value is a member in a list
 * of values.  This criteria can be represented as "&lt;expression&gt; IN (&lt;expr&gt;, ...)".
 */
public class SetCriteria extends AbstractSetCriteria {

    /** The set of value expressions */
    private Collection values;
    private boolean allConstants;

    /**
     * Constructs a default instance of this class.
     */
    public SetCriteria() {}

    /**
     * Constructs an instance of this class with the membership expression and value expressions
     * @param expression The membership expression
     * @param values   The set of value {@link org.teiid.query.sql.symbol.Expression}s
     */
    public SetCriteria( Expression expression, Collection values ) {
        set(expression,values);
    }

    /**
     * Returns the number of values in the set.
     * @return Number of values in set
     */
    public int getNumberOfValues() {
        return (this.values != null) ? this.values.size() : 0;
    }

    /**
     * Returns the set of values.  Returns an empty collection if there are
     * currently no values.
     * @return The collection of Expression values
     */
    public Collection getValues() {
        return this.values;
    }

    /**
     * Sets the values in the set.
     * @param values The set of value Expressions
     */
    public void setValues( Collection values ) {
        this.values = values;
    }

    /**
     * Sets the membership expression and the set of value expressions
     * @param expression The membership expression
     * @param values   The set of value expressions
     */
    public void set( Expression expression, Collection values ) {
        setExpression(expression);
        setValues(values);
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

        // The expHashCode method walks the set of values, combining
        // the hash code at every power of 2: 1,2,4,8,...  This is
        // much quicker than calculating hash codes for ALL values

        hc = HashCodeUtil.expHashCode(hc, getValues());
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

        if(! (obj instanceof SetCriteria)) {
            return false;
        }

        SetCriteria sc = (SetCriteria)obj;

        if (isNegated() ^ sc.isNegated()) {
            return false;
        }

        if (getValues().size() != sc.getValues().size() ||
            !EquivalenceUtil.areEqual(getExpression(), sc.getExpression())) {
            return false;
        }

        if (this.allConstants) {
            for (Expression ex : (Collection<Expression>)sc.getValues()) {
                if (!(ex instanceof Constant)) {
                    return false;
                }
                if (!values.contains(ex)) {
                    return false;
                }
            }
        } else {
            if (!getValues().containsAll(sc.getValues())) {
                return false;
            }
        }

        if (!(sc.values instanceof Set)) {
            Collection unique = null;
            if (!DataTypeManager.isHashable(sc.getExpression().getType())) {
                unique = new TreeSet(sc.getValues());
            } else {
                unique = new HashSet(sc.getValues());
            }
            if (unique.size() < sc.getValues().size()) {
                return false;
            }
        }

        return true;
    }

    /**
     * Deep copy of object
     * @return Deep copy of object
     */
    public Object clone() {
        Expression copy = null;
        if(getExpression() != null) {
            copy = (Expression) getExpression().clone();
        }

        Collection copyValues = null;
        if (isAllConstants()) {
            if (values instanceof HashSet) {
                copyValues = new LinkedHashSet();
            } else {
                //assume that it's non-hashable
                copyValues = new TreeSet();
            }
            for (Expression ex : (Collection<Expression>)values) {
                copyValues.add(ex.clone());
            }
        } else {
            copyValues = LanguageObject.Util.deepClone(values, Expression.class);
        }

        SetCriteria criteriaCopy = new SetCriteria(copy, copyValues);
        criteriaCopy.setNegated(isNegated());
        criteriaCopy.allConstants = allConstants;
        return criteriaCopy;
    }

    public boolean isAllConstants() {
        return allConstants;
    }

    public void setAllConstants(boolean allConstants) {
        this.allConstants = allConstants;
    }

}  // END CLASS
