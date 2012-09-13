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

package org.teiid.dqp.internal.process;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.process.RecordTable.ExpandingSimpleIterator;
import org.teiid.dqp.internal.process.RecordTable.SimpleIterator;
import org.teiid.dqp.internal.process.RecordTable.SimpleIteratorWrapper;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.tempdata.BaseIndexInfo;
import org.teiid.query.util.CommandContext;

abstract class BaseExtractionTable<T> {
	
	static final class ExtractionTupleSource<T> implements TupleSource {
		private final Criteria condition;
		private final SimpleIterator<T> iter;
		private final CommandContext cc;
		private final VDBMetaData vdb;
		private final TransformationMetadata metadata;
		private ArrayList<Object> rowBuffer;
		private BaseExtractionTable<T> extraction;

		ExtractionTupleSource(Criteria condition,
				SimpleIterator<T> iter, CommandContext cc, VDBMetaData vdb,
				TransformationMetadata metadata, BaseExtractionTable<T> extraction) {
			this.condition = condition;
			this.iter = iter;
			this.cc = cc;
			this.vdb = vdb;
			this.metadata = metadata;
			this.extraction = extraction;
		}

		@Override
		public List<?> nextTuple() throws TeiidComponentException,
				TeiidProcessingException {
			while (true) {
				T val = iter.next();
				if (val == null) {
					return null;
				}
				if (rowBuffer == null) {
					rowBuffer = new ArrayList<Object>(extraction.cols);
				} else {
					rowBuffer.clear();
				}
				extraction.fillRow(rowBuffer, val, vdb, metadata, cc, iter);
				if (condition == null || extraction.eval.evaluate(condition, rowBuffer)) {
					List<?> result = rowBuffer;
					rowBuffer = null;
					return result;
				}
			
			}
		}

		@Override
		public void closeSource() {
			
		}
	}

	private Evaluator eval;
	private int cols;
	
	public BaseExtractionTable(List<ElementSymbol> columns) {
		Map<Expression, Integer> map = RelationalNode.createLookupMap(columns);
		this.eval = new Evaluator(map, null, null);
		this.cols = columns.size();
	}
	
	public TupleSource processQuery(Query query, final VDBMetaData vdb, final TransformationMetadata metadata, final CommandContext cc) throws QueryMetadataException, TeiidComponentException {
		return new ExtractionTupleSource(query.getCriteria(), createIterator(vdb, metadata, cc), cc, vdb, metadata, this);
	}
	
	protected SimpleIterator<T> createIterator(final VDBMetaData vdb, final TransformationMetadata metadata, final CommandContext cc) throws QueryMetadataException, TeiidComponentException {
		return null;
	}

	protected abstract void fillRow(List<Object> row, T record, VDBMetaData vdb, TransformationMetadata metadata, CommandContext cc, SimpleIterator<T> iter);
	
}

abstract class RecordExtractionTable<T extends AbstractMetadataRecord> extends BaseExtractionTable<T> {
	private RecordTable<T> baseTable;

	public RecordExtractionTable(RecordTable<T> baseTable, List<ElementSymbol> columns) {
		super(columns);
		this.baseTable = baseTable;
	}
	
	@Override
	public TupleSource processQuery(Query query, VDBMetaData vdb,
			TransformationMetadata metadata, CommandContext cc) {
		BaseIndexInfo<?> ii = baseTable.planQuery(query, query.getCriteria());
		final SimpleIterator<T> iter = baseTable.processQuery(vdb, metadata.getMetadataStore(), ii);
		return new ExtractionTupleSource<T>(ii.getNonCoveredCriteria(), iter, cc, vdb, metadata, this);
	}
	
}

abstract class ChildRecordExtractionTable<P extends AbstractMetadataRecord, T> extends BaseExtractionTable<T> {
	private RecordTable<P> baseTable;

	public ChildRecordExtractionTable(RecordTable<P> baseTable, List<ElementSymbol> columns) {
		super(columns);
		this.baseTable = baseTable;
	}
	
	@Override
	public TupleSource processQuery(Query query, VDBMetaData vdb,
			TransformationMetadata metadata, CommandContext cc) {
		BaseIndexInfo<?> ii = baseTable.planQuery(query, query.getCriteria());
		final SimpleIterator<P> iter = baseTable.processQuery(vdb, metadata.getMetadataStore(), ii);
		while (ii.next != null) {
			ii = ii.next;
		}
		return new ExtractionTupleSource<T>(ii.getNonCoveredCriteria(), new ExpandingSimpleIterator<P, T>(iter) {
			
			SimpleIteratorWrapper<T> wrapper = new SimpleIteratorWrapper<T>(null);
			
			protected RecordTable.SimpleIterator<T> getChildIterator(P parent) {
				Collection<? extends T> children = getChildren(parent);
				if (children.isEmpty()) {
					return RecordTable.emptyIterator();
				}
				wrapper.setIterator(children.iterator());
				return wrapper;
			}
			
		}, cc, vdb, metadata, this);
	}
	
	protected abstract Collection<? extends T> getChildren(P parent);
	
}
