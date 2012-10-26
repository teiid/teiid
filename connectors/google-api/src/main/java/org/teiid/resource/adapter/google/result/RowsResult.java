package org.teiid.resource.adapter.google.result;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.resource.adapter.google.common.SheetRow;


/**
 * This class is iterable result of any batchable service.
 * 
 *  RowsResult contains logic that will retrieve batches of results so that memory consumption is optimal.
 *  
 *  This Iterable can be set so that it skips first N elements (setOffset) and/or limits the amount of 
 *  iterated elements (setLimit) 
 * @author fnguyen
 *
 */
public class RowsResult implements Iterable<SheetRow> {
	private PartialResultExecutor queryStrategy;
	private int batchSize = 0;
	
	private Integer offset = 0;
	private Integer limit = null;
	
	public RowsResult(PartialResultExecutor queryStrategy, int batchSize) {
		this.queryStrategy = queryStrategy;
		this.batchSize = batchSize;
	}
	
	public void setOffset(int i){
		offset = i;
	}

	public void setLimit(int i){
		limit = i;
	}
	
	@Override
	public Iterator<SheetRow> iterator() {
		return new BatchingRowIterator();
	}
	
	private class BatchingRowIterator implements Iterator<SheetRow> {
		private int returnedAlready = 0;
		private List<SheetRow> currentBatch = new ArrayList<SheetRow>();
		private int position = -1;
		private int batchStartIndex = 0;
		private boolean noMoreBatches = false;
		
		@Override
		public boolean hasNext() {
			if (limit != null && returnedAlready == limit)
				return false;
			
			if (position < currentBatch.size() - 1)
				return true;
			
			if (noMoreBatches)
				return false;
			
			currentBatch = queryStrategy.getResultsBatch(batchStartIndex+offset, batchSize);
			batchStartIndex+=batchSize;				
			if (currentBatch == null  || currentBatch.size()==0)
				return false;
			
			if (currentBatch.size() < batchSize)
				noMoreBatches = true;
			
			position = -1;
			return true;
		}

		@Override
		public SheetRow next() {
			returnedAlready++;
			return currentBatch.get(++position);
		}

		@Override
		public void remove() {			
		}
		
	}
}
