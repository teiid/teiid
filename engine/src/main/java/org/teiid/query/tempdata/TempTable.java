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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.STree.TupleBrowser;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.SortUtility;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.SetClauseList;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.SingleElementSymbol;

/**
 * A Teiid Temp Table
 * TODO: an update will not happen unless the tuplesource is accessed
 * TODO: better handling for blocked exceptions (should be rare)
 */
class TempTable {
	
	private class TupleBrowserTupleSource implements TupleSource {
		private final TupleBrowser browser;

		private TupleBrowserTupleSource(TupleBrowser browser) {
			this.browser = browser;
		}

		@Override
		public List<?> nextTuple() throws TeiidComponentException,
				TeiidProcessingException {
			return browser.next();
		}

		@Override
		public List<? extends Expression> getSchema() {
			return columns;
		}

		@Override
		public void closeSource() {
			
		}

		@Override
		public int available() {
			return 0;
		}
	}

	private abstract class UpdateTupleSource implements TupleSource {
		private TupleSource ts;
		protected final Map lookup;
		protected final Evaluator eval;
		private final Criteria crit;
		protected int updateCount = 0;
		protected boolean done;
		private List currentTuple;
		
		protected TupleBuffer undoLog;

		UpdateTupleSource(Criteria crit, TupleSource ts) throws TeiidComponentException {
			this.ts = ts;
			this.lookup = RelationalNode.createLookupMap(columns);
			this.eval = new Evaluator(lookup, null, null);
			this.crit = crit;
			this.undoLog = bm.createTupleBuffer(columns, sessionID, TupleSourceType.PROCESSOR);
		}
		
		void process() throws TeiidComponentException, TeiidProcessingException {
			//still have to worry about blocked exceptions...
			while (currentTuple != null || (currentTuple = ts.nextTuple()) != null) {
				if (crit == null || eval.evaluate(crit, currentTuple)) {
					tuplePassed(currentTuple);
					updateCount++;
					undoLog.addTuple(currentTuple);
				}
				currentTuple = null;
			}
		}
		
		@Override
		public List<?> nextTuple() throws TeiidComponentException,
				TeiidProcessingException {
			if (done) {
				return null;
			}
			try {
				process();
			    done = true;
			} catch (BlockedException e) {
				//this is horrible...
				throw e; 
			} finally {
				if (!done) {
					TupleSource undoTs = undoLog.createIndexedTupleSource();
					List<?> tuple = null;
					try {
						while ((tuple = undoTs.nextTuple()) != null) {
							undo(tuple);
						}
					} catch (TeiidException e) {
						
					}
				}
			}
			return Arrays.asList(updateCount);
		}

		protected abstract void tuplePassed(List tuple) throws BlockedException, TeiidComponentException, TeiidProcessingException;
		
		protected abstract void undo(List tuple) throws TeiidComponentException, TeiidProcessingException;
		
		@Override
		public List<SingleElementSymbol> getSchema() {
			return Command.getUpdateCommandSymbol();
		}

		@Override
		public int available() {
			return 0;
		}
		
		@Override
		public void closeSource() {
			ts.closeSource();
			undoLog.remove();
		}
		
	}
	
	private STree tree;
	private AtomicInteger rowId;
	private List<ElementSymbol> columns;
	private BufferManager bm;
	private String sessionID;

	TempTable(BufferManager bm, List<ElementSymbol> columns, int primaryKeyLength, String sessionID) {
		this.bm = bm;
		if (primaryKeyLength == 0) {
            ElementSymbol id = new ElementSymbol("rowId"); //$NON-NLS-1$
    		id.setType(DataTypeManager.DefaultDataClasses.INTEGER);
    		columns.add(0, id);
    		rowId = new AtomicInteger();
        	tree = bm.createSTree(columns, sessionID, TupleSourceType.PROCESSOR, 1);
        } else {
        	tree = bm.createSTree(columns, sessionID, TupleSourceType.PROCESSOR, primaryKeyLength);
        }
		this.columns = columns;
		this.sessionID = sessionID;
	}

	public TupleSource createTupleSource(List<SingleElementSymbol> cols, Criteria condition, OrderBy orderBy) throws TeiidComponentException, TeiidProcessingException {
		Map map = RelationalNode.createLookupMap(getColumns());
		Boolean direction = null;
		boolean orderByUsingIndex = false;
		if (orderBy != null && rowId == null) {
			int[] orderByIndexes = RelationalNode.getProjectionIndexes(map, orderBy.getSortKeys());
			if (orderByIndexes.length < tree.getKeyLength()) {
				orderByUsingIndex = false;
			} else {
				orderByUsingIndex = true;
				for (int i = 0; i < tree.getKeyLength(); i++) {
					if (orderByIndexes[i] != i) {
						orderByUsingIndex = false;
						break;
					}
				}
				if (orderByUsingIndex) {
					for (int i = 0; i < tree.getKeyLength(); i++) {
						OrderByItem item = orderBy.getOrderByItems().get(i);
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
		}
		if (!orderByUsingIndex) {
			direction = OrderBy.ASC;
		}
		TupleBrowser browser = createTupleBrower(null, direction);
		
		final int[] indexes = RelationalNode.getProjectionIndexes(map, cols);
		final ArrayList<SingleElementSymbol> projectedCols = new ArrayList<SingleElementSymbol>(cols);
		for (SingleElementSymbol singleElementSymbol : projectedCols) {
			if (singleElementSymbol instanceof AliasSymbol) {
				
			}
		}
		final boolean project = shouldProject(indexes);
		TupleSource ts = new TupleBrowserTupleSource(browser) {
			
			@Override
			public List<?> nextTuple() throws TeiidComponentException,
					TeiidProcessingException {
				List<?> next = super.nextTuple();
				if (next == null) {
					return null;
				}
				if (rowId != null) {
					next = next.subList(1, next.size());
				}
				if (project) {
					next = RelationalNode.projectTuple(indexes, next);
				}
				return next;
			}
			
			@Override
			public List<? extends Expression> getSchema() {
				return projectedCols;
			}
		};

		TupleBuffer tb = null;
		if (!orderByUsingIndex && orderBy != null) {
			SortUtility sort = new SortUtility(ts, orderBy.getOrderByItems(), Mode.SORT, bm, sessionID);
			tb = sort.sort();
		} else {
			tb = bm.createTupleBuffer(getColumns(), sessionID, TupleSourceType.PROCESSOR);
			List next = null;
			while ((next = ts.nextTuple()) != null) {
				tb.addTuple(next);
			}
		}
		tb.close();
		tb.setForwardOnly(true);
		return tb.createIndexedTupleSource(true);
	}

	private boolean shouldProject(final int[] indexes) {
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
	
	private TupleBrowser createTupleBrower(List<Criteria> conditions, boolean direction) {
		return tree.browse(null, null, direction);
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
	
	public TupleSource insert(List<List<Object>> tuples) throws TeiidComponentException {
        UpdateTupleSource uts = new UpdateTupleSource(null, new CollectionTupleSource(tuples.iterator(), getColumns())) {
        	
        	protected void tuplePassed(List tuple) 
        	throws BlockedException, TeiidComponentException, TeiidProcessingException {
        		if (rowId != null) {
        			tuple.add(0, rowId.getAndAdd(1));
        		}
        		insertTuple(tuple);
        	}
        	
        	@Override
        	protected void undo(List tuple) throws TeiidComponentException {
        		deleteTuple(tuple);
        	}
        	
        };
        return uts;
    }
	
	public TupleSource update(Criteria crit, final SetClauseList update) throws TeiidComponentException {
		final boolean primaryKeyChangePossible = canChangePrimaryKey(update);
		final TupleBrowser browser = createTupleBrower(null, OrderBy.ASC);
		UpdateTupleSource uts = new UpdateTupleSource(crit, new TupleBrowserTupleSource(browser)) {
			
			protected TupleBuffer changeSet;
			protected TupleSource changeSetProcessor;
			
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
			void process() throws TeiidComponentException,
					TeiidProcessingException {
				super.process();
				//existing tuples have been removed
				//changeSet contains possible updates
				if (primaryKeyChangePossible) {
					if (changeSetProcessor == null) {
						changeSetProcessor = new UpdateTupleSource(null, changeSet.createIndexedTupleSource(true)) {
							@Override
							protected void tuplePassed(List tuple) throws BlockedException,
									TeiidComponentException, TeiidProcessingException {
								insertTuple(tuple);
							}
							
							@Override
							protected void undo(List tuple) throws TeiidComponentException,
									TeiidProcessingException {
								deleteTuple(tuple);
							}
							
						};
					}
					changeSetProcessor.nextTuple(); //when this returns, we're up to date
				}
			}
			
			@Override
			protected void undo(List tuple) throws TeiidComponentException, TeiidProcessingException {
				if (primaryKeyChangePossible) {
					insertTuple(tuple);
				} else {
					updateTuple(tuple);
				}
			}
			
			@Override
			public void closeSource() {
				super.closeSource();
				if (changeSetProcessor != null) {
					changeSetProcessor.closeSource(); // causes a revert of the change set
				}
				if (changeSet != null) {
					changeSet.remove();
				}
			}
			
		};
		return uts;
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
	
	public TupleSource delete(Criteria crit) throws TeiidComponentException {
		final TupleBrowser browser = createTupleBrower(null, OrderBy.ASC);
		UpdateTupleSource uts = new UpdateTupleSource(crit, new TupleBrowserTupleSource(browser)) {
			@Override
			protected void tuplePassed(List tuple)
					throws ExpressionEvaluationException,
					BlockedException, TeiidComponentException {
				browser.removed();
				deleteTuple(tuple);
			}
			
			@Override
			protected void undo(List tuple) throws TeiidComponentException, TeiidProcessingException {
				insertTuple(tuple);
			}
		};
		return uts;
	}
	
	private void insertTuple(List<Object> list) throws TeiidComponentException, TeiidProcessingException {
		if (tree.insert(list, false) != null) {
			throw new TeiidProcessingException(QueryPlugin.Util.getString("TempTable.duplicate_key")); //$NON-NLS-1$
		}
	}
	
	private void deleteTuple(List<?> tuple) throws TeiidComponentException {
		if (tree.remove(tuple) == null) {
			throw new AssertionError("Delete failed"); //$NON-NLS-1$
		}
	}
	
	private void updateTuple(List<?> tuple) throws TeiidComponentException {
		if (tree.insert(tuple, true) == null) {
			throw new AssertionError("Update failed"); //$NON-NLS-1$
		}
	}
		
}