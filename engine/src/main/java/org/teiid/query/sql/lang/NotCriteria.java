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
import org.teiid.query.sql.LanguageVisitor;


/**
 * A logical criteria that takes the logical NOT of the contained criteria.
 * That is, if the contained criteria returns true, this criteria returns
 * false.  For example:  "NOT (element = 5)"
 */
public class NotCriteria extends LogicalCriteria {

    /** The single sub criteria */
    private Criteria criteria;

    /**
     * Constructs a default instance of this class.
     */
    public NotCriteria() {
    }

    /**
     * Constructs an instance of this class with sub-criteria.
     * @param crit Contained criteria
     */
    public NotCriteria(Criteria crit) {
        setCriteria(crit);
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }


    /**
     * Compare equality of two AtomicCriteria.
     * @param obj Other object
     * @return True if equivalent
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if(!(obj instanceof NotCriteria)) {
            return false;
        }

        return EquivalenceUtil.areEqual(getCriteria(), ((NotCriteria)obj).getCriteria());
    }

    /**
     * Get hash code
     * @return Hash code
     */
    public int hashCode() {
        return HashCodeUtil.hashCode(0, getCriteria());
    }

    /**
     * Deep copy of object
     * @return Deep copy of object
     */
    public Object clone() {
        return new NotCriteria( (Criteria) getCriteria().clone() );
    }

    /**
     * Get sub criteria
     * @return Sub criteria
     */
    public Criteria getCriteria() {
        return criteria;
    }

    /**
     * Set sub criteria
     * @param criteria Sub criteria
     */
    public void setCriteria(Criteria criteria) {
        this.criteria = criteria;
    }

}
