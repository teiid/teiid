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

package org.teiid.query.sql.util;

import java.util.Set;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.sql.symbol.Expression;



/**
 * The ValueIteratorSource lets a language object that needs a ValueIterator hold this
 * reference to the source of the ValueIterator as a reference until the ValueIterator
 * can be ready.
 *
 * @since 5.0.1
 */
public interface ValueIteratorSource {

    /**
     * Attempt to obtain a ValueIterator from this source.  If the iterator is
     * not ready yet, return null to indicate that.
     * @param valueExpression The expression we are retrieving an iterator for
     * @return ValueIterator if ready, null otherwise
     * @throws TeiidComponentException
     * @since 5.0.1
     */
    ValueIterator getValueIterator(Expression valueExpression) throws TeiidComponentException;

    Set<Object> getCachedSet(Expression valueExpression) throws TeiidComponentException, TeiidProcessingException;

    void setUnused(boolean unused);

    boolean isUnused();

    boolean inMemory();

}
