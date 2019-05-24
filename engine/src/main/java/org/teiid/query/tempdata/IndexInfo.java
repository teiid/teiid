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

package org.teiid.query.tempdata;

import java.util.List;

import org.teiid.common.buffer.TupleBrowser;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.translator.ExecutionFactory.NullOrder;

/**
 * Accumulates information about index usage.
 */
class IndexInfo extends BaseIndexInfo<TempTable> {

    TupleSource valueTs;

    public IndexInfo(TempTable table, final List<? extends Expression> projectedCols, final Criteria condition, OrderBy orderBy, boolean primary) {
        super(table, projectedCols, condition, orderBy, primary);
    }

    TupleBrowser createTupleBrowser(NullOrder nullOrder, boolean readOnly) throws TeiidComponentException {
        boolean direction = OrderBy.ASC;
        if (ordering != null) {
            LogManager.logDetail(LogConstants.CTX_DQP, "Using index for ordering"); //$NON-NLS-1$
            direction = ordering;
        }
        if (valueTs != null) {
            LogManager.logDetail(LogConstants.CTX_DQP, "Using index value set"); //$NON-NLS-1$
            return new TupleBrowser(this.table.getTree(), valueTs, direction, readOnly);
        }
        if (!valueSet.isEmpty()) {
            LogManager.logDetail(LogConstants.CTX_DQP, "Using index value set"); //$NON-NLS-1$
            sortValueSet(direction, nullOrder);
            CollectionTupleSource cts = new CollectionTupleSource(valueSet.iterator());
            return new TupleBrowser(this.table.getTree(), cts, direction, readOnly);
        }
        if (lower != null || upper != null) {
            LogManager.logDetail(LogConstants.CTX_DQP, "Using index for range query", lower, upper); //$NON-NLS-1$
        }
        return new TupleBrowser(this.table.getTree(), lower, upper, direction, readOnly);
    }

}