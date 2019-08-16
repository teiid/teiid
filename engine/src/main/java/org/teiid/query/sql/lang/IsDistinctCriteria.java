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

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.PredicateCriteria.Negatable;


/**
 * Represents criteria such as:  "&lt;expression&gt; IS DISTINCT FROM &lt;expression&gt;".
 * However due to a lack of direct support for new/old groups as row values,
 * we reference group symbols here instead.
 */
public class IsDistinctCriteria extends PredicateCriteria implements Negatable {

    private LanguageObject leftRowValue;
    private LanguageObject rightRowValue;
    /** Negation flag. Indicates whether the criteria expression contains a NOT. */
    private boolean negated;

    /**
     * Constructs a default instance of this class.
     */
    public IsDistinctCriteria() {}

    public void setLeftRowValue(LanguageObject leftRowValue) {
        this.leftRowValue = leftRowValue;
    }

    public void setRightRowValue(LanguageObject rightRowValue) {
        this.rightRowValue = rightRowValue;
    }

    public LanguageObject getLeftRowValue() {
        return leftRowValue;
    }

    public LanguageObject getRightRowValue() {
        return rightRowValue;
    }

    /**
     * Returns whether this criteria is negated.
     * @return flag indicating whether this criteria contains a NOT
     */
    public boolean isNegated() {
        return negated;
    }

    /**
     * Sets the negation flag for this criteria.
     * @param negationFlag true if this criteria contains a NOT; false otherwise
     */
    public void setNegated(boolean negationFlag) {
        negated = negationFlag;
    }

    @Override
    public void negate() {
        this.negated = !this.negated;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Get hash code.  WARNING: The hash code is based on data in the criteria.
     * If data values are changed, the hash code will change - don't hash this
     * object and change values.
     * @return Hash code for object
     */
    public int hashCode() {
        return HashCodeUtil.hashCode(negated?0:1, leftRowValue, rightRowValue);
    }

    /**
     * Compares this criteria to another object for equality
     * @param obj Other object
     * @return True if objects are equal
     */
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(! (obj instanceof IsDistinctCriteria)) {
            return false;
        }

        IsDistinctCriteria other = (IsDistinctCriteria) obj;
        if (isNegated() ^ other.isNegated()) {
            return false;
        }
        return EquivalenceUtil.areEqual(leftRowValue, other.leftRowValue)
                && EquivalenceUtil.areEqual(rightRowValue, other.rightRowValue);
    }

    /**
     * Deep copy of object
     * @return Deep copy of object
     */
    public Object clone() {
        IsDistinctCriteria criteriaCopy = new IsDistinctCriteria();
        criteriaCopy.setNegated(isNegated());
        criteriaCopy.setLeftRowValue((LanguageObject)this.leftRowValue.clone());
        criteriaCopy.setRightRowValue((LanguageObject)this.rightRowValue.clone());
        return criteriaCopy;
    }

}