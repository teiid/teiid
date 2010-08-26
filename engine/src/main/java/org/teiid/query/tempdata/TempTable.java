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
import java.util.HashSet;
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
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.SetClauseList;
import org.teiid.query.sql.symbol.Constant;
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
						newTuple.add(null);
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
			lock.readLock().lock();
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
				if (rowId != null) {
					next = next.subList(1, next.size());
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
			lock.readLock().unlock();
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
		protected final Map lookup;
		protected final Evaluator eval;
		private final Criteria crit;
		protected int updateCount = 0;
		protected List currentTuple;
		
		protected TupleBuffer undoLog;

		UpdateProcessor(Criteria crit, TupleSource ts) throws TeiidComponentException {
			this.ts = ts;
			this.lookup = RelationalNode.createLookupMap(columns);
			this.eval = new Evaluator(lookup, null, null);
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
	
	private int keyBatchSize;
	private int leafBatchSize;

	TempTable(TempMetadataID tid, BufferManager bm, List<ElementSymbol> columns, int primaryKeyLength, String sessionID) {
		this.tid = tid;
		this.bm = bm;
		if (primaryKeyLength == 0) {
            ElementSymbol id = new ElementSymbol("rowId"); //$NON-NLS-1$
    		id.setType(DataTypeManager.DefaultDataClasses.INTEGER);
    		columns.add(0, id);
    		rowId = new AtomicInteger();
        	tree = bm.createSTree(columns, sessionID, 1);
        } else {
        	tree = bm.createSTree(columns, sessionID, primaryKeyLength);
        }
		this.columns = columns;
		this.sessionID = sessionID;
		this.keyBatchSize = bm.getSchemaSize(columns);
		this.leafBatchSize = bm.getSchemaSize(columns.subList(0, primaryKeyLength));
	}
	
	
	private int reserveBuffers() {
		return bm.reserveBuffers(leafBatchSize + (tree.getHeight() - 1)*keyBatchSize, BufferReserveMode.WAIT);
	}

	public TupleSource createTupleSource(final List<? extends SingleElementSymbol> projectedCols, final Criteria condition, OrderBy orderBy) throws TeiidComponentException, TeiidProcessingException {
		Map map = RelationalNode.createLookupMap(getColumns());
		
		Boolean direction = null;
		boolean orderByUsingIndex = false;
		if (orderBy != null && rowId == null) {
			int[] orderByIndexes = RelationalNode.getProjectionIndexes(map, orderBy.getSortKeys());
			orderByUsingIndex = true;
			for (int i = 0; i < tree.getKeyLength(); i++) {
				if (orderByIndexes.length <= i) {
					break;
				}
				if (orderByIndexes[i] != i) {
					orderByUsingIndex = false;
					break;
				}
			}
			if (orderByUsingIndex) {
				for (OrderByItem item : orderBy.getOrderByItems()) {
					if (item.getNullOrdering() != null) {
						orderByUsingIndex = false;
						break;
					}
					if (item.isAscending()) {
						if (direction == null) {
							direction = OrderBy.ASC;
						} else if (direction != OrderBy.ASC) {
							orderByUsingIndex = false;
							break;
						}
					} else if (direction == null) {
						direction = OrderBy.DESC;
					} else if (direction != OrderBy.DESC) {
						orderByUsingIndex = false;
						break;
					}
				}
			}
		}
		if (!orderByUsingIndex) {
			direction = OrderBy.ASC;
		}
		
		TupleBrowser browser = createTupleBrower(condition, direction);
		TupleSource ts = new QueryTupleSource(browser, map, projectedCols, condition);
		
		boolean usingQueryTupleSource = false;
		try {
			TupleBuffer tb = null;
			if (!orderByUsingIndex && orderBy != null) {
				SortUtility sort = new SortUtility(ts, orderBy.getOrderByItems(), Mode.SORT, bm, sessionID, projectedCols);
				tb = sort.sort();
			} else if (!updatable) {
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

	private TupleBrowser createTupleBrower(Criteria condition, boolean direction) throws TeiidComponentException {
		List<Object> lower = null;
		List<Object> upper = null;
		List<List<Object>> values = null;
		if (condition != null && rowId == null) {
			IndexCondition[] indexConditions = IndexCondition.getIndexConditions(condition, columns.subList(0, tree.getKeyLength()));
			for (int i = 0; i < indexConditions.length; i++) {
				IndexCondition indexCondition = indexConditions[i];
				if (indexCondition.lower != null) {
					if (i == 0) {
						lower = new ArrayList<Object>(tree.getKeyLength());
						lower.add(indexCondition.lower.getValue());
					} if (lower != null && lower.size() == i) {
						lower.add(indexCondition.lower.getValue());
					}
				} 
				if (indexCondition.upper != null) {
					if (i == 0) {
						upper = new ArrayList<Object>(tree.getKeyLength());
						upper.add(indexCondition.upper.getValue());
					} else if (upper != null && upper.size() == i) {
						upper.add(indexCondition.upper.getValue());
					}
				} 
				if (!indexCondition.valueSet.isEmpty()) {
					if (i == 0) {
						values = new ArrayList<List<Object>>();
						for (Constant constant : indexCondition.valueSet) {
							List<Object> value = new ArrayList<Object>(tree.getKeyLength());
							value.add(constant.getValue());
							values.add(value);
						}
					} else if (values != null && values.size() == 1 && values.iterator().next().size() == i && indexCondition.valueSet.size() == 1) {
						values.iterator().next().add(indexCondition.valueSet.first().getValue());
					}
				}
			}
		}
		if (values != null) {
			return new TupleBrowser(this.tree, values, direction);
		}
		return new TupleBrowser(this.tree, lower, upper, direction);
	}
	
	public int getRowCount() {
		return tree.getRowCount();
	}
	
	public int truncate() {
		return tree.truncate();
	}
	
	public void remove() {
		tree.remove();
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
    				newTuple.set((Integer)lookup.get(entry.getKey()), eval.evaluate(entry.getValue(), tuple));
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
	
	private void insertTuple(List<Object> list, boolean ordered) throws TeiidComponentException, TeiidProcessingException {
		if (tree.insert(list, ordered?InsertMode.ORDERED:InsertMode.NEW) != null) {
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
				return tree.remove(tuple);
			} 
			return tree.insert(tuple, InsertMode.UPDATE);
		} finally {
			lock.writeLock().unlock();
		}
	}
	
	private void updateTuple(List<?> tuple) throws TeiidComponentException {
		if (tree.insert(tuple, InsertMode.UPDATE) == null) {
			throw new AssertionError("Update failed"); //$NON-NLS-1$
		}
	}
	
	void setPreferMemory(boolean preferMemory) {
		this.tree.setPreferMemory(preferMemory);
	}
	
	void setUpdatable(boolean updatable) {
		this.updatable = updatable;
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

}