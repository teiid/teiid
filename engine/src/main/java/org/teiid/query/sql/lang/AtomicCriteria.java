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


/**
 * This abstract class represents an atomic logical criteria.  An
 * atomic criteria operates on a single other criteria and evaluates
 * to true or false during processing.
 */
public abstract class AtomicCriteria extends LogicalCriteria {

    /** The single sub criteria */
    private Criteria criteria;

    /**
     * Constructs a default instance of this class.
     */
    protected AtomicCriteria() {
    }

    /**
     * Constructs an instance of this class with a single sub-criteria.
     */
    protected AtomicCriteria(Criteria crit) {
        setCriteria(crit);
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

    /**
     * Deep copy of object
     * @return Deep copy of object
     */
    public abstract Object clone();

}

