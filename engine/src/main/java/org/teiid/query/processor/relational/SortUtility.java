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

package org.teiid.query.processor.relational;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.TreeSet;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.Assertion;
import org.teiid.language.SortSpecification.NullOrdering;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.symbol.Expression;


/**
 * Implements several modes of a multi-pass sort.
 */
public class SortUtility {
	
	public enum Mode {
		SORT,
		/** Removes duplicates, no sort elements need to be specified.
		 *  may perform additional passes as new batches become available 
		 */
		DUP_REMOVE, 
		/** Removes duplicates, but guarantees order based upon the sort elements.
		 */
		DUP_REMOVE_SORT
	}
	
	/**
	 * state holder for the merge algorithm
	 */
	private class SortedSublist implements Comparable<SortedSublist> {
		List<?> tuple;
		int index;
		IndexedTupleSource its;
		int limit = Integer.MAX_VALUE;
		
		@Override
		public int compareTo(SortedSublist o) {
			//reverse the comparison, so that removal of the lowest is a low cost operation
			return -comparator.compare(this.tuple, o.tuple);
		}
		
		@Override
		public String toString() {
			return index + " " + tuple; //$NON-NLS-1$
		}
	}

	//constructor state
    private TupleSource source;
    private Mode mode;
    private BufferManager bufferManager;
    private String groupName;
    private List<? extends Expression> schema;
    private int schemaSize;
	private ListNestedSortComparator comparator;

    private TupleBuffer output;
    private boolean doneReading;
    private int phase = INITIAL_SORT;
    private List<TupleBuffer> activeTupleBuffers = new ArrayList<TupleBuffer>();
    private int masterSortIndex;
    
    private int collected;

    // Phase constants for readability
    private static final int INITIAL_SORT = 1;
    private static final int MERGE = 2;
    private static final int DONE = 3;
	private Collection<List<?>> workingTuples;
    
    public SortUtility(TupleSource sourceID, List<OrderByItem> items, Mode mode, BufferManager bufferMgr,
                        String groupName, List schema) {
        this.source = sourceID;
        this.mode = mode;
        this.bufferManager = bufferMgr;
        this.groupName = groupName;
        this.schema = schema;
        this.schemaSize = bufferManager.getSchemaSize(this.schema);
        int distinctIndex = items != null? items.size() - 1:0;
        List<Expression> sortElements = null;
        List<Boolean> sortTypes = null;
        List<NullOrdering> nullOrderings = null;
        if (items == null) {
    		sortElements = (List<Expression>) this.schema;
    		sortTypes = Collections.nCopies(sortElements.size(), OrderBy.ASC);
        } else {
        	sortElements = new ArrayList(items.size());
        	sortTypes = new ArrayList<Boolean>(items.size());
        	nullOrderings = new ArrayList<NullOrdering>(items.size());
        	for (OrderByItem orderByItem : items) {
				sortElements.add(orderByItem.getSymbol());
				sortTypes.add(orderByItem.isAscending());
				nullOrderings.add(orderByItem.getNullOrdering());
			}
            if (items.size() < schema.size() && mode != Mode.SORT) {
	        	List<Expression> toAdd = new ArrayList<Expression>(schema);
	        	toAdd.removeAll(sortElements);
	        	sortElements.addAll(toAdd);
	        	sortTypes.addAll(Collections.nCopies(sortElements.size() - sortTypes.size(), OrderBy.ASC));
	        	nullOrderings.addAll(Collections.nCopies(sortElements.size() - nullOrderings.size(), (NullOrdering)null));
            }
        }
        
        int[] cols = new int[sortElements.size()];
        for (ListIterator<Expression> iter = sortElements.listIterator(); iter.hasNext();) {
        	Expression elem = iter.next();
            
            cols[iter.previousIndex()] = schema.indexOf(elem);
            Assertion.assertTrue(cols[iter.previousIndex()] != -1);
        }
        this.comparator = new ListNestedSortComparator(cols, sortTypes);
        this.comparator.setDistinctIndex(distinctIndex);
        this.comparator.setNullOrdering(nullOrderings);
    }
    
    public SortUtility(TupleSource ts, List expressions, List<Boolean> types,
			Mode mode, BufferManager bufferManager, String connectionID, List schema) {
		this(ts, new OrderBy(expressions, types).getOrderByItems(), mode, bufferManager, connectionID, schema);
	}

	public boolean isDone() {
    	return this.doneReading && this.phase == DONE;
    }
    
    public TupleBuffer sort()
        throws TeiidComponentException, TeiidProcessingException {

        if(this.phase == INITIAL_SORT) {
            initialSort();
        }
        
        if(this.phase == MERGE) {
            mergePhase();
        }
        if (this.output != null) {
        	return this.output;
        }
        return this.activeTupleBuffers.get(0);
    }
    
    public List<TupleBuffer> onePassSort() throws TeiidComponentException, TeiidProcessingException {
    	assert this.mode != Mode.DUP_REMOVE;
    	
    	if(this.phase == INITIAL_SORT) {
            initialSort();
        }
    	
    	return activeTupleBuffers;
    }

	private TupleBuffer createTupleBuffer() throws TeiidComponentException {
		TupleBuffer tb = bufferManager.createTupleBuffer(this.schema, this.groupName, TupleSourceType.PROCESSOR);
		tb.setForwardOnly(true);
		return tb;
	}
    
	/**
	 * creates sorted sublists stored in tuplebuffers
	 */
    protected void initialSort() throws TeiidComponentException, TeiidProcessingException {
    	while(!doneReading) {
    		if (workingTuples == null) {
	            if (mode == Mode.SORT) {
	            	workingTuples = new ArrayList<List<?>>();
	            } else {
	            	workingTuples = new TreeSet<List<?>>(comparator);
	            }
    		}
    		
            int totalReservedBuffers = 0;
            try {
	            int maxRows = this.bufferManager.getProcessorBatchSize();
		        while(!doneReading) {
		        	//attempt to reserve more working memory if there are additional rows available before blocking
		        	if (workingTuples.size() >= maxRows) {
	        			int reserved = bufferManager.reserveBuffers(schemaSize, 
	        					(totalReservedBuffers + schemaSize <= bufferManager.getMaxProcessingKB())?BufferReserveMode.FORCE:BufferReserveMode.NO_WAIT);
	        			if (reserved != schemaSize) {
		        			break;
		        		} 
		        		totalReservedBuffers += reserved;
		        		maxRows += bufferManager.getProcessorBatchSize();	
		        	}
		            try {
		            	List<?> tuple = source.nextTuple();
		            	
		            	if (tuple == null) {
		            		doneReading = true;
		            		break;
		            	}
	                    if (workingTuples.add(tuple)) {
	                    	this.collected++;
	                    }
		            } catch(BlockedException e) {
		            	if (workingTuples.size() >= bufferManager.getProcessorBatchSize()) {
		            		break;
		            	}
		            	if (mode != Mode.DUP_REMOVE  
		            			|| (this.output != null && collected < this.output.getRowCount() * 2) 
		            			|| (this.output == null && this.workingTuples.isEmpty() && this.activeTupleBuffers.isEmpty())) {
	            			throw e; //block if no work can be performed
		            	}
		            	break;
		            } 
		        } 
		
		        if(workingTuples.isEmpty()) {
		        	break;
		        }
			
		        TupleBuffer sublist = createTupleBuffer();
		        activeTupleBuffers.add(sublist);
		        if (this.mode == Mode.SORT) {
		        	//perform a stable sort
		    		Collections.sort((List<List<?>>)workingTuples, comparator);
		        }
		        for (List<?> list : workingTuples) {
					sublist.addTuple(list);
				}
		        workingTuples = null;
	            
		        sublist.saveBatch();
            } finally {
        		bufferManager.releaseBuffers(totalReservedBuffers);
            }
        }
    	
    	if (this.activeTupleBuffers.isEmpty()) {
            activeTupleBuffers.add(createTupleBuffer());
        }  
    	this.collected = 0;
        this.phase = MERGE;
    }

    protected void mergePhase() throws TeiidComponentException, TeiidProcessingException {
    	while(this.activeTupleBuffers.size() > 1) {    		
    		ArrayList<SortedSublist> sublists = new ArrayList<SortedSublist>(activeTupleBuffers.size());
            
            TupleBuffer merged = createTupleBuffer();

            int desiredSpace = activeTupleBuffers.size() * schemaSize;
            int reserved = Math.min(desiredSpace, this.bufferManager.getMaxProcessingKB());
            bufferManager.reserveBuffers(reserved, BufferReserveMode.FORCE);
            if (desiredSpace > reserved) {
            	reserved += bufferManager.reserveBuffers(desiredSpace - reserved, BufferReserveMode.WAIT);
            }
            int maxSortIndex = Math.max(2, reserved / schemaSize); //always allow progress
            //release any partial excess
            int release = reserved % schemaSize > 0 ? 1 : 0;
            bufferManager.releaseBuffers(release);
            reserved -= release;
            try {
	        	if (LogManager.isMessageToBeRecorded(org.teiid.logging.LogConstants.CTX_DQP, MessageLevel.TRACE)) {
	            	LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, "Merging", maxSortIndex, "sublists out of", activeTupleBuffers.size()); //$NON-NLS-1$ //$NON-NLS-2$
	            }
	        	// initialize the sublists with the min value
	            for(int i = 0; i<maxSortIndex; i++) { 
	             	TupleBuffer activeID = activeTupleBuffers.get(i);
	             	SortedSublist sortedSublist = new SortedSublist();
	            	sortedSublist.its = activeID.createIndexedTupleSource();
	            	sortedSublist.index = i;
	            	if (activeID == output) {
	            		sortedSublist.limit = output.getRowCount();
	            	}
	            	incrementWorkingTuple(sublists, sortedSublist);
	            }
	            
	            // iteratively process the lowest tuple
	            while (sublists.size() > 0) {
	            	SortedSublist sortedSublist = sublists.remove(sublists.size() - 1);
	        		merged.addTuple(sortedSublist.tuple);
	                if (this.output != null && sortedSublist.index > masterSortIndex) {
	                	this.output.addTuple(sortedSublist.tuple); //a new distinct row
	            	}
	            	incrementWorkingTuple(sublists, sortedSublist);
	            }                
	
	            // Remove merged sublists
	            for(int i=0; i<maxSortIndex; i++) {
	            	TupleBuffer id = activeTupleBuffers.remove(0);
	            	if (id != this.output) {
	            		id.remove();
	            	}
	            }
	            merged.saveBatch();
	            this.activeTupleBuffers.add(merged);           
	            masterSortIndex = masterSortIndex - maxSortIndex + 1;
	            if (masterSortIndex < 0) {
	            	masterSortIndex = this.activeTupleBuffers.size() - 1;
	            }
            } finally {
            	this.bufferManager.releaseBuffers(reserved);
            }
        }
    	
        // Close sorted source (all others have been removed)
        if (doneReading) {
        	if (this.output != null) {
	        	this.output.close();
	        	TupleBuffer last = activeTupleBuffers.remove(0);
	        	if (output != last) {
	        		last.remove();
	        	}
	        } else {
	        	activeTupleBuffers.get(0).close();
	        	activeTupleBuffers.get(0).setForwardOnly(false);
	        }
	        this.phase = DONE;
	        return;
        }
    	Assertion.assertTrue(mode == Mode.DUP_REMOVE);
    	if (this.output == null) {
    		this.output = activeTupleBuffers.get(0);
    		this.output.setForwardOnly(false);
    	}
    	this.phase = INITIAL_SORT;
    }

	private void incrementWorkingTuple(ArrayList<SortedSublist> subLists, SortedSublist sortedSublist) throws TeiidComponentException, TeiidProcessingException {
		while (true) {
			sortedSublist.tuple = null;
			if (sortedSublist.limit < sortedSublist.its.getCurrentIndex()) {
				return; //special case for still reading the output tuplebuffer
			}
			try {
				sortedSublist.tuple = sortedSublist.its.nextTuple();
	        } catch (BlockedException e) {
	        	//intermediate sources aren't closed
	        }  
	        if (sortedSublist.tuple == null) {
	        	return; // done with this sublist
	        }
			int index = Collections.binarySearch(subLists, sortedSublist);
			if (index < 0) {
				subLists.add(-index - 1, sortedSublist);
				return;
			}
			if (mode == Mode.SORT) {
				subLists.add(index, sortedSublist);
				return;
			} 
			/* In dup removal mode we need to ensure that a sublist other than the master is incremented
			 */
			if (mode == Mode.DUP_REMOVE && this.output != null && sortedSublist.index == masterSortIndex) {
				SortedSublist dup = subLists.get(index);
				subLists.set(index, sortedSublist);
				sortedSublist = dup;
			}
		}
	} 

    public boolean isDistinct() {
    	return this.comparator.isDistinct();
    }
    
}
