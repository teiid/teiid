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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.TupleBrowser;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.STree.InsertMode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.SortUtility;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.SetClauseList;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.SingleElementSymbol;

/**
 * A Teiid Temp Table
 * TODO: in this implementation blocked exceptions will not happen
 *       allowing for subquery evaluation though would cause pauses
 */
class TempTable {
	
	private final class InsertUpdateProcessor extends UpdateProcessor {
		
		private boolean addRowId;
		private int[] indexes;
		
		private InsertUpdateProcessor(TupleSource ts, boolean addRowId, int[] indexes)
				throws TeiidComponentException {
			super(null, ts);
			this.addRowId = addRowId;
			this.indexes = indexes;
		}

		@Override
		protected void tuplePassed(List tuple) throws BlockedException,
				TeiidComponentException, TeiidProcessingException {
			if (indexes != null) {
				List<Object> newTuple = new ArrayList<Object>(columns.size());
				if (addRowId) {
					newTuple.add(rowId.getAndIncrement());
				}
				for (int i = 0; i < indexes.length; i++) {
					if (indexes[i] == -1) {
						AtomicInteger sequence = sequences.get(i);
						if (sequence != null) {
							newTuple.add(sequence.getAndIncrement());
						} else {
							newTuple.add(null);
						}
					} else {
						newTuple.add(tuple.get(indexes[i]));
					}
				}
				tuple = newTuple;
			} else if (addRowId) {
				tuple = new ArrayList<Object>(tuple);
				tuple.add(0, rowId.getAndIncrement());
			}
			currentTuple = tuple;
			for (int i : notNull) {
				if (tuple.get(i) == null) {
					throw new TeiidProcessingException(QueryPlugin.Util.getString("TempTable.not_null", columns.get(i)));
				}
			}
			insertTuple(tuple, addRowId);
		}

		@Override
		protected void undo(List tuple) throws TeiidComponentException,
				TeiidProcessingException {
			deleteTuple(tuple);
		}
	}

	private final class QueryTupleSource implements TupleSource {
		private final Evaluator eval;
		private final Criteria condition;
		private final boolean project;
		private final int[] indexes;
		private int reserved;
		private TupleBrowser browser;

		private QueryTupleSource(TupleBrowser browser, Map map,
				List<? extends SingleElementSymbol> projectedCols, Criteria condition) {
			this.browser = browser;
			this.indexes = RelationalNode.getProjectionIndexes(map, projectedCols);
			this.eval = new Evaluator(map, null, null);
			this.condition = condition;
			this.project = shouldProject();
			this.reserved = reserveBuffers();
			if (updatable) {
				lock.readLock().lock();
			}
		}

		@Override
		public List<?> nextTuple() throws TeiidComponentException,
				TeiidProcessingException {
			for (;;) {
				List<?> next = browser.nextTuple();
				if (next == null) {
					bm.releaseBuffers(reserved);
					reserved = 0;
					return null;
				}
				if (condition != null && !eval.evaluate(condition, next)) {
					continue;
				}
				if (project) {
					next = RelationalNode.projectTuple(indexes, next);
				}
				return next;
			}
		}
		
		@Override
		public void closeSource() {
			if (updatable) {
				lock.readLock().unlock();
			}
			bm.releaseBuffers(reserved);
			reserved = 0;
			browser.closeSource();
		}
		
		private boolean shouldProject() {
			if (indexes.length == getColumns().size()) {
				for (int i = 0; i < indexes.length; i++) {
					if (indexes[i] != i) {
						return true;
					}
				}
				return false;
			} 
			return true;
		}

	}

	private abstract class UpdateProcessor {
		private TupleSource ts;
		protected final Evaluator eval;
		private final Criteria crit;
		protected int updateCount = 0;
		protected List currentTuple;
		
		protected TupleBuffer undoLog;

		UpdateProcessor(Criteria crit, TupleSource ts) throws TeiidComponentException {
			this.ts = ts;
			this.eval = new Evaluator(columnMap, null, null);
			this.crit = crit;
			this.undoLog = bm.createTupleBuffer(columns, sessionID, TupleSourceType.PROCESSOR);
		}
		
		int process() throws ExpressionEvaluationException, TeiidComponentException, TeiidProcessingException {
			int reserved = reserveBuffers();
			boolean held = lock.writeLock().isHeldByCurrentThread();
			lock.writeLock().lock();
			boolean success = false;
			try {
				while (currentTuple != null || (currentTuple = ts.nextTuple()) != null) {
					if (crit == null || eval.evaluate(crit, currentTuple)) {
						tuplePassed(currentTuple);
						updateCount++;
						undoLog.addTuple(currentTuple);
					}
					currentTuple = null;
				}
				bm.releaseBuffers(reserved);
				reserved = 0;
				success();
				success = true;
			} finally {
				bm.releaseBuffers(reserved);
				try {
					if (!success) {
						undoLog.setFinal(true);
						TupleSource undoTs = undoLog.createIndexedTupleSource();
						List<?> tuple = null;
						while ((tuple = undoTs.nextTuple()) != null) {
							undo(tuple);
						}
					}
				} catch (TeiidException e) {
					LogManager.logError(LogConstants.CTX_DQP, e, e.getMessage());
				} finally {
					if (!held) {
						lock.writeLock().unlock();
					}
					close();
				}
			}
			return updateCount;
		}

		@SuppressWarnings("unused")
		void success() throws TeiidComponentException, ExpressionEvaluationException, TeiidProcessingException {}

		protected abstract void tuplePassed(List tuple) throws BlockedException, TeiidComponentException, TeiidProcessingException;
		
		protected abstract void undo(List tuple) throws TeiidComponentException, TeiidProcessingException;
		
		public void close() {
			ts.closeSource();
			undoLog.remove();
		}
		
	}
	
	private STree tree;
	private AtomicInteger rowId;
	private List<ElementSymbol> columns;
	private BufferManager bm;
	private String sessionID;
	private TempMetadataID tid;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private boolean updatable = true;
	private LinkedHashMap<List<ElementSymbol>, TempTable> indexTables;
	
	private int keyBatchSize;
	private int leafBatchSize;
	private Map columnMap;
	
	private List<Integer> notNull = new LinkedList<Integer>();
	private Map<Integer, AtomicInteger> sequences;
	private int uniqueColIndex;

	TempTable(TempMetadataID tid, BufferManager bm, List<ElementSymbol> columns, int primaryKeyLength, String sessionID) {
		this.tid = tid;
		this.bm = bm;
		int startIndex = 0;
		if (primaryKeyLength == 0) {
			startIndex = 1;
            ElementSymbol id = new ElementSymbol("rowId"); //$NON-NLS-1$
    		id.setType(DataTypeManager.DefaultDataClasses.INTEGER);
    		columns.add(0, id);
    		rowId = new AtomicInteger();
        	tree = bm.createSTree(columns, sessionID, 1);
        } else {
        	this.uniqueColIndex = primaryKeyLength;
        	tree = bm.createSTree(columns, sessionID, primaryKeyLength);
        }
		this.columnMap = RelationalNode.createLookupMap(columns);
		this.columns = columns;
		if (!tid.getElements().isEmpty()) {
			//not relevant for indexes
			for (int i = startIndex; i < columns.size(); i++) {
				TempMetadataID col = tid.getElements().get(i - startIndex);
				if (col.isAutoIncrement()) {
					if (this.sequences == null) {
						this.sequences = new HashMap<Integer, AtomicInteger>();
					}
					sequences.put(i, new AtomicInteger(1));
				}
				if (col.isNotNull()) {
					notNull.add(i);
				}
			}
		}
		if (this.sequences == null) {
			this.sequences = Collections.emptyMap();
		}
		this.sessionID = sessionID;
		this.keyBatchSize = bm.getSchemaSize(columns);
		this.leafBatchSize = bm.getSchemaSize(columns.subList(0, primaryKeyLength));
	}
	
	void addIndex(List<ElementSymbol> indexColumns, boolean unique) throws TeiidComponentException, TeiidProcessingException {
		List<ElementSymbol> keyColumns = columns.subList(0, tree.getKeyLength());
		if (keyColumns.equals(indexColumns) || (indexTables != null && indexTables.containsKey(indexColumns))) {
			return;
		}
		List<ElementSymbol> allColumns = new ArrayList<ElementSymbol>(indexColumns);
		for (ElementSymbol elementSymbol : keyColumns) {
			if (allColumns.indexOf(elementSymbol) < 0) {
				allColumns.add(elementSymbol);
			}
		}
		TempTable indexTable = new TempTable(new TempMetadataID("idx", Collections.EMPTY_LIST), this.bm, allColumns, allColumns.size(), this.sessionID); //$NON-NLS-1$
		indexTable.setPreferMemory(this.tree.isPreferMemory());
		indexTable.lock = this.lock;
		if (unique) {
			indexTable.uniqueColIndex = indexColumns.size();
		}
		if (indexTables == null) {
			indexTables = new LinkedHashMap<List<ElementSymbol>, TempTable>();
			indexTables.put(indexColumns, indexTable);
		}
		//TODO: ordered insert optimization
		TupleSource ts = createTupleSource(allColumns, null, null);
		indexTable.insert(ts, allColumns);
		indexTable.getTree().compact();
	}
	
	private int reserveBuffers() {
		return bm.reserveBuffers(leafBatchSize + (tree.getHeight() - 1)*keyBatchSize, BufferReserveMode.FORCE);
	}

	public TupleSource createTupleSource(final List<? extends SingleElementSymbol> projectedCols, final Criteria condition, OrderBy orderBy) throws TeiidComponentException, TeiidProcessingException {
		//special handling for count(*)
		boolean agg = false;
		for (SingleElementSymbol singleElementSymbol : projectedCols) {
			if (singleElementSymbol instanceof AggregateSymbol) {
				agg = true;
				break;
			}
		}
		if (agg) {
			if (condition == null) {
				int count = this.getRowCount();
				return new CollectionTupleSource(Arrays.asList(Collections.nCopies(projectedCols.size(), count)).iterator());
			}
			orderBy = null;
		}
		IndexInfo primary = new IndexInfo(this, projectedCols, condition, orderBy, true);
		IndexInfo ii = primary;
		if (indexTables != null && (condition != null || orderBy != null) && ii.valueSet.size() != 1) {
			LogManager.logDetail(LogConstants.CTX_DQP, "Considering indexes on table", this, "for query", projectedCols, condition, orderBy); //$NON-NLS-1$ //$NON-NLS-2$
			int rowCost = this.tree.getRowCount();
			int bestCost = estimateCost(orderBy, ii, rowCost);
			for (TempTable table : this.indexTables.values()) {
				IndexInfo secondary = new IndexInfo(table, projectedCols, condition, orderBy, false);
				int cost = estimateCost(orderBy, secondary, rowCost);
				if (cost < bestCost) {
					ii = secondary;
					bestCost = cost;
				}
			}
			LogManager.logDetail(LogConstants.CTX_DQP, "Choose index", ii.table, "covering:", ii.coveredCriteria,"ordering:", ii.ordering); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			if (ii.covering) {
				return ii.table.createTupleSource(projectedCols, condition, orderBy, ii, agg);
			}
			List<ElementSymbol> pkColumns = this.columns.subList(0, this.tree.getKeyLength());
			if (ii.ordering != null) {
				//use order and join
				primary.valueTs = ii.table.createTupleSource(pkColumns, 
						Criteria.combineCriteria(ii.coveredCriteria), orderBy, ii, agg);
				primary.ordering = null;
				return createTupleSource(projectedCols, Criteria.combineCriteria(ii.nonCoveredCriteria), null, primary, agg);
			} 
			//order by pk to localize lookup costs, then join
			OrderBy pkOrderBy = new OrderBy();
			for (ElementSymbol elementSymbol : pkColumns) {
				pkOrderBy.addVariable(elementSymbol);
			}
			primary.valueTs = ii.table.createTupleSource(pkColumns, 
					Criteria.combineCriteria(ii.coveredCriteria), pkOrderBy, ii, agg);
			return createTupleSource(projectedCols, Criteria.combineCriteria(ii.nonCoveredCriteria), orderBy, primary, agg);
		}
		return createTupleSource(projectedCols, condition, orderBy, ii, agg);
	}

	private TupleSource createTupleSource(
			final List<? extends SingleElementSymbol> projectedCols,
			final Criteria condition, OrderBy orderBy, IndexInfo ii, boolean agg)
			throws TeiidComponentException, TeiidProcessingException {
		TupleBrowser browser = ii.createTupleBrowser();
		TupleSource ts = new QueryTupleSource(browser, columnMap, agg?getColumns():projectedCols, condition);
		
		boolean usingQueryTupleSource = false;
		try {
			TupleBuffer tb = null;
			if (ii.ordering == null && orderBy != null) {
				SortUtility sort = new SortUtility(ts, orderBy.getOrderByItems(), Mode.SORT, bm, sessionID, projectedCols);
				tb = sort.sort();
			} else if (agg) {
				int count = 0;
				while (ts.nextTuple() != null) {
					count++;
				}
				return new CollectionTupleSource(Arrays.asList(Collections.nCopies(projectedCols.size(), count)).iterator());
			} else if (updatable) {
				tb = bm.createTupleBuffer(projectedCols, sessionID, TupleSourceType.PROCESSOR);
				List<?> next = null;
				while ((next = ts.nextTuple()) != null) {
					tb.addTuple(next);
				}
			} else {
				usingQueryTupleSource = true;
				return ts;
			}
			tb.close();
			return tb.createIndexedTupleSource(true);
		} finally {
			if (!usingQueryTupleSource) {
				//ensure the buffers get released
				ts.closeSource();
			}
		}
	}

	/**
	 * TODO: this could easily use statistics - the tree level 1 would be an ideal place
	 * to compute them, since it minimizes page loads, and is a random sample.
	 * TODO: this should also factor in the block size
	 */
	private int estimateCost(OrderBy orderBy, IndexInfo ii, int rowCost) {
		int initialCost = rowCost;
		if (ii.valueSet.size() != 0) {
			int length = ii.valueSet.get(0).size();
			rowCost = ii.valueSet.size() * (ii.table.getPkLength() - length + 1);
			if (ii.table.uniqueColIndex != length) {
				rowCost *= 3;
			}
		} else if (ii.upper != null) {
			rowCost /= 3;
		} else if (ii.lower != null) {
			rowCost /= 3;
		}
		int additionalCost = Math.max(0, initialCost - rowCost);
		int cost = Math.min(initialCost, Math.max(1, rowCost));
		if (cost > 1 && (!ii.covering || (orderBy != null && ii.ordering == null))) {
			cost *= (32 - Integer.numberOfLeadingZeros(cost - 1));
		}
		return cost + additionalCost;
	}

	private TupleBrowser createTupleBrower(Criteria condition, boolean direction) throws TeiidComponentException {
		IndexInfo ii = new IndexInfo(this, null, condition, null, true);
		ii.ordering = direction;
		return ii.createTupleBrowser();
	}
	
	public int getRowCount() {
		return tree.getRowCount();
	}
	
	public int truncate() {
		return tree.truncate();
	}
	
	public void remove() {
		tree.remove();
		if (this.indexTables != null) {
			for (TempTable indexTable : this.indexTables.values()) {
				indexTable.remove();
			}
		}
	}
	
	public List<ElementSymbol> getColumns() {
		if (rowId != null) {
			return columns.subList(1, columns.size());
		}
		return columns;
	}
	
	public TupleSource insert(TupleSource tuples, final List<ElementSymbol> variables) throws TeiidComponentException, ExpressionEvaluationException, TeiidProcessingException {
		List<ElementSymbol> cols = getColumns();
		final int[] indexes = new int[cols.size()];
		boolean shouldProject = false;
		for (int i = 0; i < cols.size(); i++) {
			indexes[i] = variables.indexOf(cols.get(i));
			shouldProject |= (indexes[i] != i);
		}
        UpdateProcessor up = new InsertUpdateProcessor(tuples, rowId != null, shouldProject?indexes:null);
        int updateCount = up.process();
        tid.setCardinality(tree.getRowCount());
        return CollectionTupleSource.createUpdateCountTupleSource(updateCount);
    }
	
	public TupleSource update(Criteria crit, final SetClauseList update) throws TeiidComponentException, ExpressionEvaluationException, TeiidProcessingException {
		final boolean primaryKeyChangePossible = canChangePrimaryKey(update);
		final TupleBrowser browser = createTupleBrower(crit, OrderBy.ASC);
		UpdateProcessor up = new UpdateProcessor(crit, browser) {
			
			protected TupleBuffer changeSet;
			protected UpdateProcessor changeSetProcessor;
			
			@Override
			protected void tuplePassed(List tuple)
					throws ExpressionEvaluationException,
					BlockedException, TeiidComponentException {
				List<Object> newTuple = new ArrayList<Object>(tuple);
    			for (Map.Entry<ElementSymbol, Expression> entry : update.getClauseMap().entrySet()) {
    				newTuple.set((Integer)columnMap.get(entry.getKey()), eval.evaluate(entry.getValue(), tuple));
    			}
    			if (primaryKeyChangePossible) {
    				browser.removed();
    				deleteTuple(tuple);
    				if (changeSet == null) {
    					changeSet = bm.createTupleBuffer(columns, sessionID, TupleSourceType.PROCESSOR);
    				}
    				changeSet.addTuple(newTuple);
    			} else {
    				browser.update(newTuple);
    			}
			}
			
			@Override
			protected void undo(List tuple) throws TeiidComponentException, TeiidProcessingException {
				if (primaryKeyChangePossible) {
					insertTuple(tuple, false);
				} else {
					updateTuple(tuple);
				}
			}
			
			@Override
			void success() throws TeiidComponentException, ExpressionEvaluationException, TeiidProcessingException {
				//existing tuples have been removed
				//changeSet contains possible updates
				if (primaryKeyChangePossible) {
					if (changeSetProcessor == null) {
						changeSetProcessor = new InsertUpdateProcessor(changeSet.createIndexedTupleSource(true), false, null);
					}
					changeSetProcessor.process(); //when this returns, we're up to date
				}
			}
			
			@Override
			public void close() {
				super.close();
				if (changeSetProcessor != null) {
					changeSetProcessor.close(); // causes a revert of the change set
				}
				if (changeSet != null) {
					changeSet.remove();
				}
			}
			
		};
		int updateCount = up.process();
		return CollectionTupleSource.createUpdateCountTupleSource(updateCount);
	}

	private boolean canChangePrimaryKey(final SetClauseList update) {
		if (rowId == null) {
			Set<ElementSymbol> affectedColumns = new HashSet<ElementSymbol>(update.getClauseMap().keySet());
			affectedColumns.retainAll(columns.subList(0, tree.getKeyLength()));
			if (!affectedColumns.isEmpty()) {
				return true;
			}
		}
		return false;
	}
	
	public TupleSource delete(Criteria crit) throws TeiidComponentException, ExpressionEvaluationException, TeiidProcessingException {
		final TupleBrowser browser = createTupleBrower(crit, OrderBy.ASC);
		UpdateProcessor up = new UpdateProcessor(crit, browser) {
			@Override
			protected void tuplePassed(List tuple)
					throws ExpressionEvaluationException,
					BlockedException, TeiidComponentException {
				browser.removed();
				deleteTuple(tuple);
			}
			
			@Override
			protected void undo(List tuple) throws TeiidComponentException, TeiidProcessingException {
				insertTuple(tuple, false);
			}
		};
		int updateCount = up.process();
		tid.setCardinality(tree.getRowCount());
		return CollectionTupleSource.createUpdateCountTupleSource(updateCount);
	}
	
	private void insertTuple(List<?> list, boolean ordered) throws TeiidComponentException, TeiidProcessingException {
		if (tree.insert(list, ordered?InsertMode.ORDERED:InsertMode.NEW, -1) != null) {
			throw new TeiidProcessingException(QueryPlugin.Util.getString("TempTable.duplicate_key")); //$NON-NLS-1$
		}
	}
	
	private void deleteTuple(List<?> tuple) throws TeiidComponentException {
		if (tree.remove(tuple) == null) {
			throw new AssertionError("Delete failed"); //$NON-NLS-1$
		}
	}
	
	List<?> updateTuple(List<?> tuple, boolean remove) throws TeiidComponentException {
		try {
			lock.writeLock().lock();
			if (remove) {
				List<?> result = tree.remove(tuple);
				if (result == null) {
					return null;
				}
				if (indexTables != null) {
					for (TempTable index : this.indexTables.values()) {
						tuple = RelationalNode.projectTuple(RelationalNode.getProjectionIndexes(index.getColumnMap(), index.columns), result);
						index.tree.remove(tuple);
					}
				}
				return result;
			} 
			List<?> result = tree.insert(tuple, InsertMode.UPDATE, -1);
			if (indexTables != null) {
				for (TempTable index : this.indexTables.values()) {
					tuple = RelationalNode.projectTuple(RelationalNode.getProjectionIndexes(index.getColumnMap(), index.columns), tuple);
					index.tree.insert(tuple, InsertMode.UPDATE, -1);
				}
			}
			return result;
		} finally {
			lock.writeLock().unlock();
		}
	}
	
	private void updateTuple(List<?> tuple) throws TeiidComponentException {
		if (tree.insert(tuple, InsertMode.UPDATE, -1) == null) {
			throw new AssertionError("Update failed"); //$NON-NLS-1$
		}
	}
	
	void setPreferMemory(boolean preferMemory) {
		this.tree.setPreferMemory(preferMemory);
	}
	
	void setUpdatable(boolean updatable) {
		this.updatable = updatable;
		if (this.indexTables != null) {
			for (TempTable index : this.indexTables.values()) {
				index.setUpdatable(updatable);
			}
		}
	}
	
	CacheHint getCacheHint() {
		return this.tid.getCacheHint();
	}
	
	int getPkLength() {
		if (rowId != null) {
			return 0;
		}
		return this.tree.getKeyLength();
	}
	
	public boolean isUpdatable() {
		return updatable;
	}
	
	@Override
	public String toString() {
		return tid.getID() + " (" + columns + ")\n"; //$NON-NLS-1$ //$NON-NLS-2$
	}

	Map getColumnMap() {
		return this.columnMap;
	}
	
	STree getTree() {
		return tree;
	}

}