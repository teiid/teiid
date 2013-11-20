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
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleBuffer.TupleBufferTupleSource;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.language.SortSpecification.NullOrdering;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.symbol.Expression;


/**
 * Implements several modes of a multi-pass sort.
 * 
 * TODO: could consider using an index for dup_removal and maintaining a separate output buffer
 * TODO: release the tuple buffer in the last merge pass if sublists will fit in processing batch size
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
		TupleBufferTupleSource its;
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
    private int batchSize;
	private ListNestedSortComparator comparator;
	private int targetRowCount;

    private TupleBuffer output;
    private boolean doneReading;
    private int phase = INITIAL_SORT;
    private List<TupleBuffer> activeTupleBuffers = new ArrayList<TupleBuffer>();
    private int masterSortIndex;
    
    private int processed;

    // Phase constants for readability
    private static final int INITIAL_SORT = 1;
    private static final int MERGE = 2;
    private static final int DONE = 3;
	private TupleBuffer workingBuffer;
	private long[] attempts = new long[2];
	private boolean nonBlocking;
	
	private static boolean STABLE_SORT = PropertiesUtils.getBooleanProperty(System.getProperties(), "org.teiid.requireStableSort", false); //$NON-NLS-1$
	
	private boolean stableSort = STABLE_SORT;
    
    public SortUtility(TupleSource sourceID, List<OrderByItem> items, Mode mode, BufferManager bufferMgr,
                        String groupName, List<? extends Expression> schema) {
        List<Expression> sortElements = null;
        List<Boolean> sortTypes = null;
        List<NullOrdering> nullOrderings = null;
        int distinctIndex = -1;
        if (items == null) {
    		sortElements = (List<Expression>) schema;
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
            if (items.size() < schema.size() && mode == Mode.DUP_REMOVE_SORT) {
	        	List<Expression> toAdd = new ArrayList<Expression>(schema);
	        	toAdd.removeAll(sortElements);
	        	sortElements.addAll(toAdd);
	        	sortTypes.addAll(Collections.nCopies(sortElements.size() - sortTypes.size(), OrderBy.ASC));
	        	nullOrderings.addAll(Collections.nCopies(sortElements.size() - nullOrderings.size(), (NullOrdering)null));
	        	//this path should be for join processing, which can check the isDistinct flag.
	        	//that needs the proper index based upon the original sort columns, not based upon making the whole set distinct
	        	distinctIndex = items.size() - 1;
            }
        }
        
        int[] cols = new int[sortElements.size()];
        for (ListIterator<Expression> iter = sortElements.listIterator(); iter.hasNext();) {
        	Expression elem = iter.next();
            
            cols[iter.previousIndex()] = schema.indexOf(elem);
            Assertion.assertTrue(cols[iter.previousIndex()] != -1);
        }
        init(sourceID, mode, bufferMgr, groupName, schema, sortTypes,
				nullOrderings, cols);
        if (distinctIndex != -1) {
        	this.comparator.setDistinctIndex(distinctIndex);
        }
    }
    
    public SortUtility(TupleSource sourceID, Mode mode, BufferManager bufferMgr,
			String groupName, List<? extends Expression> schema,
			List<Boolean> sortTypes, List<NullOrdering> nullOrderings,
			int[] cols) {
    	init(sourceID, mode, bufferMgr, groupName, schema, sortTypes, nullOrderings, cols);
    }

	private void init(TupleSource sourceID, Mode mode, BufferManager bufferMgr,
			String groupName, List<? extends Expression> schema,
			List<Boolean> sortTypes, List<NullOrdering> nullOrderings,
			int[] cols) {
		this.source = sourceID;
        this.mode = mode;
        this.bufferManager = bufferMgr;
        this.groupName = groupName;
        this.schema = schema;
        this.schemaSize = bufferManager.getSchemaSize(this.schema);
        this.batchSize = bufferManager.getProcessorBatchSize(this.schema);
        this.targetRowCount = Math.max(bufferManager.getMaxProcessingSize()/this.schemaSize, 2)*this.batchSize;
        this.comparator = new ListNestedSortComparator(cols, sortTypes);
        int distinctIndex = cols.length - 1;
        this.comparator.setDistinctIndex(distinctIndex);
        this.comparator.setNullOrdering(nullOrderings);
    }
    
    public SortUtility(TupleSource ts, List<? extends Expression> expressions, List<Boolean> types,
			Mode mode, BufferManager bufferManager, String connectionID, List schema) {
		this(ts, new OrderBy(expressions, types).getOrderByItems(), mode, bufferManager, connectionID, schema);
	}

    public TupleBuffer sort()
        throws TeiidComponentException, TeiidProcessingException {
    	boolean success = false;
    	try {
	        if(this.phase == INITIAL_SORT) {
	            initialSort(false);
	        }
	        
	        if(this.phase == MERGE) {
	            mergePhase();
	        }
	        success = true;
	        if (this.output != null) {
	        	return this.output;
	        }
	        return this.activeTupleBuffers.get(0);
    	} catch (BlockedException e) {
    		success = true;
    		throw e;
    	} finally {
        	if (!success) {
        		remove();
        	}
        }
    }
    
    public List<TupleBuffer> onePassSort() throws TeiidComponentException, TeiidProcessingException {
    	boolean success = false;
    	assert this.mode != Mode.DUP_REMOVE;
    	try {
	    	if(this.phase == INITIAL_SORT) {
	            initialSort(true);
	        }
	    	
	    	for (TupleBuffer tb : activeTupleBuffers) {
				tb.close();
			}
	    	success = true;
	    	return activeTupleBuffers;
    	} catch (BlockedException e) {
    		success = true;
    		throw e;
    	} finally {
    		if (!success) {
    			remove();
    		}
    	}
    }
    
	private TupleBuffer createTupleBuffer() throws TeiidComponentException {
		TupleBuffer tb = bufferManager.createTupleBuffer(this.schema, this.groupName, TupleSourceType.PROCESSOR);
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
			LogManager.logDetail(LogConstants.CTX_DQP, "Created intermediate sort buffer", tb); //$NON-NLS-1$
		}
		tb.setForwardOnly(true);
		return tb;
	}
    
	/**
	 * creates sorted sublists stored in tuplebuffers
	 */
    protected void initialSort(boolean onePass) throws TeiidComponentException, TeiidProcessingException {
    	outer: while (!doneReading) {
    		
    		if (this.source != null) {
	    		//sub-phase 1 - build up a working buffer of tuples
	    		if (this.workingBuffer == null) {
	    			this.workingBuffer = createTupleBuffer();
	    		}
	    		
	    		while (!doneReading) {
		            try {
		            	List<?> tuple = source.nextTuple();
		            	
		            	if (tuple == null) {
		            		doneReading = true;
		            		break;
		            	}
		            	this.workingBuffer.addTuple(tuple);
		            } catch(BlockedException e) {
		            	/*there are three cases here
		            	 * 1. a fully blocking sort (optionally dup removal)
		            	 * 2. a streaming dup removal
		            	 * 3. a one pass sort (for grace join like processing)
		            	 */
		            	if (!onePass && mode != Mode.DUP_REMOVE) {
		            		throw e; //read fully before processing
		            	}
		            	if (!onePass) {
		            		//streaming dup remove - we have to balance latency vs. performance
		            		//each merge phase is a full scan over the intermediate results
		            		if (this.output != null && this.workingBuffer.getRowCount() < 2*this.processed) {
		            			throw e;
		            		}
		            	} else {
		            		//we're trying to create intermediate buffers that will comfortably be small memory sorts
		            		if (this.workingBuffer.getRowCount() < this.targetRowCount) {
		            			throw e;
		            		}	
		            	}
		            	break outer; //there's processing that we can do
		            } 
	    		}
    		} else {
    			doneReading = true;
    		}
        }
    	
		//sub-phase 2 - perform a memory sort on the workingbuffer/source
    	int totalReservedBuffers = 0;
        try {
    		int maxRows = this.batchSize;
    		Collection<List<?>> workingTuples = null;
            boolean done = false;
			/*
			 * we can balance the work between the initial / multi-pass sort based upon the row count
			 * and an updated estimate of the batch memory size 
			 */
			this.workingBuffer.close();
			schemaSize = Math.max(1, this.workingBuffer.getRowSizeEstimate()*this.batchSize);
			long memorySpaceNeeded = workingBuffer.getRowCount()*(long)this.workingBuffer.getRowSizeEstimate();
			if (onePass) {
				//one pass just needs small sub-lists
				memorySpaceNeeded = Math.min(memorySpaceNeeded, bufferManager.getMaxProcessingSize());
			}
			totalReservedBuffers = bufferManager.reserveBuffers(Math.min(bufferManager.getMaxProcessingSize(), (int)Math.min(memorySpaceNeeded, Integer.MAX_VALUE)), BufferReserveMode.FORCE);
			if (totalReservedBuffers != memorySpaceNeeded) {
				int processingSublists = Math.max(2, bufferManager.getMaxProcessingSize()/schemaSize);
				int desiredSpace = (int)Math.min(Integer.MAX_VALUE, (workingBuffer.getRowCount()/processingSublists + (workingBuffer.getRowCount()%processingSublists))*(long)this.workingBuffer.getRowSizeEstimate());
				if (desiredSpace > totalReservedBuffers) {
					totalReservedBuffers += bufferManager.reserveBuffers(desiredSpace - totalReservedBuffers, BufferReserveMode.NO_WAIT);
					//TODO: wait to force 2/3 pass processing
				} else if (memorySpaceNeeded <= Integer.MAX_VALUE) {
					totalReservedBuffers += bufferManager.reserveBuffers((int)memorySpaceNeeded - totalReservedBuffers, BufferReserveMode.NO_WAIT);
				}
				if (totalReservedBuffers > schemaSize) {
					int additional = totalReservedBuffers%schemaSize;
					totalReservedBuffers-=additional;
					//release any excess
		            bufferManager.releaseBuffers(additional);
				}
			}
			TupleBufferTupleSource ts = workingBuffer.createIndexedTupleSource(source != null);
			ts.setReverse((!stableSort || mode == Mode.DUP_REMOVE) && workingBuffer.getRowCount() > this.batchSize);
			processed+=this.workingBuffer.getRowCount();
			maxRows = Math.max(1, (totalReservedBuffers/schemaSize))*batchSize;
            if (mode == Mode.SORT) {
            	workingTuples = new ArrayList<List<?>>();
            } else {
            	workingTuples = new TreeSet<List<?>>(comparator);
            }
            outer: while (!done) {
                while(!done) {
		        	if (workingTuples.size() >= maxRows) {
	        			break;
		        	}
	            	List<?> tuple = ts.nextTuple();
	            	
	            	if (tuple == null) {
	            		done = true;
	            		if(workingTuples.isEmpty()) {
				        	break outer;
				        }
	            		break;
	            	}
                    workingTuples.add(tuple);
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
		        workingTuples.clear();
		        sublist.saveBatch();
            }
        } catch (BlockedException e) {
        	Assertion.failed("should not block during memory sublist sorting"); //$NON-NLS-1$
        } finally {
    		bufferManager.releaseBuffers(totalReservedBuffers);
    		if (this.workingBuffer != null) {
    			if (this.source != null) {
    				this.workingBuffer.remove();
    			}
        		this.workingBuffer = null;
    		}
        }
    	
    	if (this.activeTupleBuffers.isEmpty()) {
            activeTupleBuffers.add(createTupleBuffer());
        }  
        this.phase = MERGE;
    }

    public void setWorkingBuffer(TupleBuffer workingBuffer) {
		this.workingBuffer = workingBuffer;
	}
    
    protected void mergePhase() throws TeiidComponentException, TeiidProcessingException {
        long desiredSpace = activeTupleBuffers.size() * (long)schemaSize;
        int toForce = (int)Math.min(desiredSpace, Math.max(2*schemaSize, this.bufferManager.getMaxProcessingSize()));
        int reserved = 0;
        
        if (desiredSpace > toForce) {
        	try {
	        	int subLists = Math.max(2, this.bufferManager.getMaxProcessingSize()/schemaSize);
	        	int twoPass = subLists * subLists;
	        	if (twoPass < activeTupleBuffers.size()) {
	        		//wait for 2-pass
	    			int needed = (int)Math.ceil(Math.pow(activeTupleBuffers.size(), .5));
	    			while (activeTupleBuffers.size()/needed + activeTupleBuffers.size()%needed > needed) {
	    				needed++;
	    			}
	    	        reserved += bufferManager.reserveBuffersBlocking(needed * schemaSize - toForce, attempts, false);
	        		if (reserved == 0 && twoPass*subLists < activeTupleBuffers.size()) {
	        			//force 3-pass
	        			needed = (int)Math.ceil(Math.pow(activeTupleBuffers.size(), 1/3d));
	        			while (activeTupleBuffers.size()/(needed*needed) + activeTupleBuffers.size()%needed > needed) {
		    				needed++;
		    			}	
	        	        reserved += bufferManager.reserveBuffersBlocking(needed * schemaSize - toForce, attempts, true);
	        		}
	        	} else if (desiredSpace < Integer.MAX_VALUE) {
	        		//wait for 1-pass
	        		reserved += bufferManager.reserveBuffersBlocking((int)desiredSpace - toForce, attempts, false);
	        	}
        	} catch (BlockedException be) {
        		if (!nonBlocking) {
        			throw be;
        		}
        	}
        }
        int total = reserved + toForce;
        if (total > schemaSize) {
            toForce -= total % schemaSize;
        }
        reserved += bufferManager.reserveBuffers(toForce, BufferReserveMode.FORCE);
        
        try {
        	while(this.activeTupleBuffers.size() > 1) {    		
	    		ArrayList<SortedSublist> sublists = new ArrayList<SortedSublist>(activeTupleBuffers.size());
	            
	            TupleBuffer merged = createTupleBuffer();

	            desiredSpace = activeTupleBuffers.size() * (long)schemaSize;
	            if (desiredSpace < reserved) {
	            	bufferManager.releaseBuffers(reserved - (int)desiredSpace);
	            	reserved = (int)desiredSpace;
	            }
	            int maxSortIndex = Math.max(2, reserved / schemaSize); //always allow progress
	            
            	if (LogManager.isMessageToBeRecorded(org.teiid.logging.LogConstants.CTX_DQP, MessageLevel.TRACE)) {
	            	LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, "Merging", maxSortIndex, "sublists out of", activeTupleBuffers.size()); //$NON-NLS-1$ //$NON-NLS-2$
	            }
	        	// initialize the sublists with the min value
	            for(int i = 0; i<maxSortIndex; i++) { 
	             	TupleBuffer activeID = activeTupleBuffers.get(i);
	             	SortedSublist sortedSublist = new SortedSublist();
	            	sortedSublist.its = activeID.createIndexedTupleSource();
	            	sortedSublist.its.setNoBlocking(true);
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
	                if (this.output != null && masterSortIndex < maxSortIndex && sortedSublist.index != masterSortIndex) {
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
	            masterSortIndex = masterSortIndex - maxSortIndex;
	            if (masterSortIndex < 0) {
	            	masterSortIndex = this.activeTupleBuffers.size() - 1;
	            }
    		}
        } finally {
        	this.bufferManager.releaseBuffers(reserved);
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
			sortedSublist.tuple = sortedSublist.its.nextTuple();
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

	public void remove() {
		if (workingBuffer != null && source != null) {
			workingBuffer.remove();
			workingBuffer = null;
		}
		if (!this.activeTupleBuffers.isEmpty()) {
			//these can be leaked with a single pass, but
			//they should not be reused whole
			for (int i = 0; i < this.activeTupleBuffers.size(); i++) {
				TupleBuffer tb = this.activeTupleBuffers.get(i);
				if (tb == output || (i == 0 && phase == DONE)) {
					continue;
				}
				tb.remove();
			}
			this.activeTupleBuffers.clear();
		}
		this.output = null;
	}

	public void setNonBlocking(boolean b) {
		this.nonBlocking = b;
	}
	
	public void setStableSort(boolean stableSort) {
		this.stableSort = stableSort;
	}
	
	void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
    
}
