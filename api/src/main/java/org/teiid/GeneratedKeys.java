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

package org.teiid;

import java.util.Iterator;
import java.util.List;

public interface GeneratedKeys {

    /**
     * Add a generated key to this result.  The list values must match the class types of this result.
     * @param vals
     */
    void addKey(List<?> vals);

    /**
     * Get the column names of this result.
     * @return
     */
    String[] getColumnNames();

    /**
     * Get the column types of this result.
     * @return
     */
    Class<?>[] getColumnTypes();

    /**
     * Get an iterator to the keys added to this result.  The iterator is not guaranteed to be thread-safe
     * with respect to the {@link #addKey(List)} method.
     * @return
     */
    Iterator<List<?>> getKeyIterator();

}
