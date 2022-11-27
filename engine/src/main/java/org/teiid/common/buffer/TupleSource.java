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

package org.teiid.common.buffer;

import java.util.List;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;


/**
 * <p>A cursored source of tuples.  The implementation will likely be closely
 * bound to a {@link BufferManager} implementation - it will work with it
 * to use {@link TupleBatch TupleBatches} behind the scenes.
 */
public interface TupleSource {

    /**
     * Returns the next tuple
     * @return the next tuple (a List object), or <code>null</code> if
     * there are no more tuples.
     * @throws TeiidComponentException indicating a non-business
     * exception such as a communication exception, or other such
     * nondeterministic exception
     */
    List<?> nextTuple()
        throws TeiidComponentException, TeiidProcessingException;

    /**
     * Closes the Tuple Source.
     */
    void closeSource();

}
