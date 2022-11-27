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
import java.util.Iterator;

import org.teiid.core.TeiidComponentException;
import org.teiid.query.sql.util.ValueIterator;


public class CollectionValueIterator implements ValueIterator {

    private Collection vals;

    private Iterator instance = null;

    public CollectionValueIterator(Collection vals) {
        this.vals = vals;
    }

    /**
     * @see org.teiid.query.sql.util.ValueIterator#hasNext()
     * @since 4.3
     */
    public boolean hasNext() throws TeiidComponentException {
        if (instance == null) {
            this.instance = vals.iterator();
        }
        return this.instance.hasNext();
    }

    /**
     * @see org.teiid.query.sql.util.ValueIterator#next()
     * @since 4.3
     */
    public Object next() throws TeiidComponentException {
        if (instance == null) {
            this.instance = vals.iterator();
        }
        return this.instance.next();
    }

    /**
     * @see org.teiid.query.sql.util.ValueIterator#reset()
     * @since 4.3
     */
    public void reset() {
        this.instance = null;
    }

}