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
 * <p>This abstract class represents a predicate criteria, which involves some
 * statement involving expressions and can be evaluated in the context of
 * a single row of data to be either true or false.
 *
 * <p>Predicate criteria can be composed into more sophisticated criteria
 * using "OR" and "AND" logical operators.
 */
public abstract class PredicateCriteria extends Criteria {

    public interface Negatable {

        void negate();

    }

    /**
     * Constructs a default instance of this class.
     */
    public PredicateCriteria() {
    }

    /**
     * Deep copy of object
     * @return Deep copy of object
     */
    public abstract Object clone();

}  // END CLASS
