/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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

/**
 * Accumulates information about index usage.
 */
class IndexInfo extends BaseIndexInfo<TempTable> {
	
	TupleSource valueTs;
	
	public IndexInfo(TempTable table, final List<? extends Expression> projectedCols, final Criteria condition, OrderBy orderBy, boolean primary) {
		super(table, projectedCols, condition, orderBy, primary);
	}

	TupleBrowser createTupleBrowser() throws TeiidComponentException {
		boolean direction = OrderBy.ASC;
		if (ordering != null) {
			LogManager.logDetail(LogConstants.CTX_DQP, "Using index for ordering"); //$NON-NLS-1$
			direction = ordering;
		}
		if (valueTs != null) {
			LogManager.logDetail(LogConstants.CTX_DQP, "Using index value set"); //$NON-NLS-1$
			return new TupleBrowser(this.table.getTree(), valueTs, direction);
		}
		if (!valueSet.isEmpty()) {
			LogManager.logDetail(LogConstants.CTX_DQP, "Using index value set"); //$NON-NLS-1$
			if (ordering != null) {
				sortValueSet(direction);
			}
			CollectionTupleSource cts = new CollectionTupleSource(valueSet.iterator());
			return new TupleBrowser(this.table.getTree(), cts, direction);
		}
		if (lower != null || upper != null) {
			LogManager.logDetail(LogConstants.CTX_DQP, "Using index for range query", lower, upper); //$NON-NLS-1$
		} 
		return new TupleBrowser(this.table.getTree(), lower, upper, direction);
	}
	
}