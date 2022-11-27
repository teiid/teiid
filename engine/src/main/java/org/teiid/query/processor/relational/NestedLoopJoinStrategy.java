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

package org.teiid.query.processor.relational;

import java.util.List;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.relational.SourceState.ImplicitBuffer;


/**
 * Nested loop is currently implemented as a degenerate case of merge join.
 *
 * Only for use with Full, Left, Inner, and Cross joins
 *
 */
public class NestedLoopJoinStrategy extends MergeJoinStrategy {

    public NestedLoopJoinStrategy() {
        super(SortOption.ALREADY_SORTED, SortOption.ALREADY_SORTED, false);
    }

    /**
     * @see org.teiid.query.processor.relational.MergeJoinStrategy#clone()
     */
    @Override
    public NestedLoopJoinStrategy clone() {
        return new NestedLoopJoinStrategy();
    }

    /**
     * @see org.teiid.query.processor.relational.MergeJoinStrategy#compare(java.util.List, java.util.List, int[], int[])
     */
    @Override
    protected int compare(List leftProbe,
                          List rightProbe,
                          int[] leftExpressionIndecies,
                          int[] rightExpressionIndecies) {
        return 0; // there are no expressions in nested loop joins, comparison is meaningless
    }

    @Override
    protected void loadRight() throws TeiidComponentException,
            TeiidProcessingException {
        this.rightSource.setImplicitBuffer(ImplicitBuffer.FULL);
    }

    /**
     * @see org.teiid.query.processor.relational.MergeJoinStrategy#toString()
     */
    @Override
    public String toString() {
        return "NESTED LOOP JOIN"; //$NON-NLS-1$
    }

}