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

package com.metamatrix.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.metamatrix.core.util.Assertion;

/** 
 * @since 4.3
 */
public class BatchResults {
		
	static class Batch{
	    private List[] batch;
	    private int beginRow;
	    private int endRow;
	    private boolean isLast;
	    
	    Batch(List[] batch, int beginRow, int endRow, boolean isLast){
	        this.batch = batch;
	        this.beginRow = beginRow;
	        this.endRow = endRow;
	        this.isLast = isLast;
	    }
	    
	    int getLength() {
	        return batch.length;
	    }
	    
	    List getRow(int index) {
	        return batch[index - beginRow];
	    }
	    
	    int getBeginRow() {
	        return beginRow;
	    }
	    
	    int getEndRow() {
	        return endRow;
	    }
	    
	    boolean isLast() {
	        return isLast;
	    }
	    
	}
		
    private static final int SAVED_BATCHES = 3;

    private ArrayList<Batch> batches = new ArrayList<Batch>(SAVED_BATCHES + 1);
    
    private int currentRowNumber;
    private int lastRowNumber = -1;
    private int highestRowNumber;
    private BatchFetcher batchFetcher;
    private int fetchSize;
    
    public BatchResults(List[] batch, int beginRow, int endRow, boolean isLast, int fetchSize) {
    	this.fetchSize = fetchSize;
    	this.setBatch(new Batch(batch, beginRow, endRow, isLast));
    }
    
    public BatchResults(BatchFetcher batchFetcher, int fetchSize, Batch batch) {
		this.batchFetcher = batchFetcher;
		this.fetchSize = fetchSize;
		this.setBatch(batch);
	}

    /**
     * Moving forward through the results it's expected that the batches are arbitrarily size.
     * Moving backward through the results it's expected that the batches will match the fetch size.
     */
	public List getCurrentRow() throws SQLException {
    	if (currentRowNumber == 0 || (lastRowNumber != -1 && currentRowNumber > lastRowNumber)) {
    		return null;
    	}
    	int closestMax = currentRowNumber + fetchSize - 1;
    	int closestMin = Math.max(1, currentRowNumber - fetchSize + 1);
    	for (int i = 0; i < batches.size(); i++) {
    		Batch batch = batches.get(i);
    		if (currentRowNumber < batch.getBeginRow()) {
    			if (i != SAVED_BATCHES - 1) {
    				closestMax = Math.min(batch.getBeginRow(), closestMax);
    			}
    			continue;
    		}
			if (currentRowNumber > batch.getEndRow()) {
				if (i != SAVED_BATCHES - 1) {
					closestMin = Math.max(batch.getEndRow(), closestMin);
				}
				continue;
			}
			if (i != 0) {
				batches.add(0, batches.remove(i));
			}
			return batch.getRow(currentRowNumber);
		}
    	if (closestMax - currentRowNumber >= currentRowNumber - closestMin) {
        	requestBatchAndWait(currentRowNumber, closestMax);
    	} else {
    		requestBatchAndWait(currentRowNumber, closestMin);
    	}
    	Batch batch = batches.get(0);
        return batch.getRow(currentRowNumber);
    }
    
	private void requestNextBatch() throws SQLException {
		requestBatchAndWait(highestRowNumber + 1, highestRowNumber + fetchSize);
	}
    
    public boolean next() throws SQLException{
    	if (hasNext()) {
    		currentRowNumber++;
            return true;
    	}
    	
    	if (currentRowNumber == highestRowNumber) {
    		currentRowNumber++;
    	}
    	
    	return false;
    }
    
    public boolean hasPrevious() {
        return (currentRowNumber != 0 && currentRowNumber != 1);
    }
    
    public boolean previous() {
        if (hasPrevious()) {
        	currentRowNumber--;
        	return true;
        }
        
        if (this.currentRowNumber == 1) {
        	currentRowNumber--;
        }
        
        return false;
    }
                
    public void setBatchFetcher(BatchFetcher batchFetcher) {
        this.batchFetcher = batchFetcher;
    }
    
    public int getFetchSize() {
        return fetchSize;
    } 
    
    public boolean absolute(int row) throws SQLException {
    	return absolute(row, 0);
    }
    
    public boolean absolute(int row, int offset) throws SQLException {
        if(row == 0) {
            currentRowNumber = 0;
            return false;
        }
        
        if (row > 0) {
        	//row is greater than highest, but the last row is not known
        	while (row + offset > highestRowNumber && lastRowNumber == -1) {
        		requestNextBatch();
        	}

        	if (row + offset <= highestRowNumber) {
        		currentRowNumber = row;
        		return true;
        	}

    		currentRowNumber = lastRowNumber + 1 - offset;
    		return false;
        }
        
        row -= offset;
        
        while (lastRowNumber == -1) {
        	requestNextBatch();
        }
        
        int positiveRow = lastRowNumber + row + 1;
        
        if (positiveRow <= 0) {
        	currentRowNumber = 0;
        	return false;
        }
        
        currentRowNumber = positiveRow;
        return true;
    }
    
    public int getCurrentRowNumber() {
        return currentRowNumber;
    }
        
    private void requestBatchAndWait(int beginRow, int endRow) throws SQLException{
    	setBatch(batchFetcher.requestBatch(beginRow, endRow));
	}

	private void setBatch(Batch batch) {
		Assertion.assertTrue(batch.getLength() != 0 || batch.isLast());
		if (batch.isLast()) {
            this.lastRowNumber = batch.getEndRow();
        }
        highestRowNumber = Math.max(batch.getEndRow(), highestRowNumber); 
        this.batches.add(0, batch);
        if (batches.size() > SAVED_BATCHES) {
        	batches.remove(SAVED_BATCHES);
        }
	}

	public boolean hasNext() throws SQLException {
		return hasNext(1);
	}

	public boolean hasNext(int next) throws SQLException {
    	while (currentRowNumber + next > highestRowNumber && lastRowNumber == -1) {
			requestNextBatch();
        }
        
        return (currentRowNumber + next <= highestRowNumber);
	}

	public int getFinalRowNumber() {
		return lastRowNumber;
	}

	public int getHighestRowNumber() {
		return highestRowNumber;
	}
       
}
