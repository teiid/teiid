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

import org.teiid.core.TeiidComponentException;

/**
 * <p>Interface for interating through Expressions or values.  It may return
 * instances of Expression (which then have to be evaluated) or it may
 * return some Object constant value - this should be checked for after
 * calling {@link #next next}
 *
 * <p>This interface is meant to abstract the details of how the values are
 * stored and retrieved, if they are even stored in memory or not, etc. etc.
 * An implementation instance may or may not be resettable and therefore
 * reusable - see {@link #reset reset}.
 */
public interface ValueIterator{

    /**
     * Returns <tt>true</tt> if the iteration has more values. (In other
     * words, returns <tt>true</tt> if <tt>next</tt> would return a value
     * rather than throwing an exception.)
     * @return <tt>true</tt> if this ValueIterator has more values.
     * @throws TeiidComponentException indicating a non business-
     * related Exception such as a service or bean being unavailable, or
     * a communication failure.
     */
    boolean hasNext()
    throws TeiidComponentException;

    /**
     * Returns the next Expression or Object value in the interation.
     * @return the next Expression or Object value in the iteration.
     * @throws TeiidComponentException indicating a non business-
     * related Exception such as a service or bean being unavailable, or
     * a communication failure.
     */
    Object next()
    throws TeiidComponentException;

    /**
     * Optional reset method - allows a single instance of a
     * ValueIterator implementation to be resettable, such that the
     * next call to {@link #next next} returns the first element in
     * the iteration (if any).  This method should be able to be
     * called at any point during the lifecycle of a ValueIterator
     * instance.
     * @throws UnsupportedOperationException if this method is not
     * implemented
     */
    void reset();
}
