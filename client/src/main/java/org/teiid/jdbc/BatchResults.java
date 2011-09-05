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

package org.teiid.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.teiid.core.util.Assertion;


/** 
 * @since 4.3
 */
public class BatchResults {
	
	public interface BatchFetcher {
		Batch requestBatch(int beginRow) throws SQLException;
	}
		
	static class Batch{
	    private List[] batch;
	    private int beginRow;
	    private int endRow;
	    private boolean isLast;
	    private int lastRow = -1;
	    
	    Batch(List[] batch, int beginRow, int endRow, boolean isLast){
	        this.batch = batch;
	        this.beginRow = beginRow;
	        this.endRow = endRow;
	        this.isLast = isLast;
	        if (isLast) {
	        	this.lastRow = endRow;
	        }
	    }
	    
	    int getLastRow() {
			return lastRow;
		}
	    
	    void setLastRow(int lastRow) {
			this.lastRow = lastRow;
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
	
	static final int DEFAULT_SAVED_BATCHES = 3;
		
    private ArrayList<Batch> batches = new ArrayList<Batch>();
    
    private int currentRowNumber;
    private List<?> currentRow;
    private int lastRowNumber = -1;
    private int highestRowNumber;
    private BatchFetcher batchFetcher;
    private int savedBatches = DEFAULT_SAVED_BATCHES;
    
    public BatchResults(List[] batch, int beginRow, int endRow, boolean isLast) {
    	this.setBatch(new Batch(batch, beginRow, endRow, isLast));
    }
    
    public BatchResults(BatchFetcher batchFetcher, Batch batch, int savedBatches) {
		this.batchFetcher = batchFetcher;
		this.savedBatches = savedBatches;
		this.setBatch(batch);
	}

    /**
     * Moving forward through the results it's expected that the batches are arbitrarily size.
     * Moving backward through the results it's expected that the batches will match the fetch size.
     */
	public List getCurrentRow() throws SQLException {
		if (currentRow != null) {
			return currentRow;
		}
    	if (this.currentRowNumber == 0 || (lastRowNumber != -1 && this.currentRowNumber > lastRowNumber)) {
    		return null;
    	}
    	for (int i = 0; i < batches.size(); i++) {
    		Batch batch = batches.get(i);
    		if (this.currentRowNumber < batch.getBeginRow()) {
    			continue;
    		}
			if (this.currentRowNumber > batch.getEndRow()) {
				continue;
			}
			if (i != 0) {
				batches.add(0, batches.remove(i));
			}
			currentRow = batch.getRow(this.currentRowNumber);
			return currentRow;
		}
		requestBatchAndWait(this.currentRowNumber);
    	Batch batch = batches.get(0);
        currentRow = batch.getRow(this.currentRowNumber);
        return currentRow;
    }
    
	private void requestNextBatch() throws SQLException {
		requestBatchAndWait(highestRowNumber + 1);
	}
    
    public boolean next() throws SQLException{
    	if (hasNext()) {
    		setCurrentRowNumber(this.currentRowNumber + 1);
    		getCurrentRow();
            return true;
    	}
    	
    	if (this.currentRowNumber == highestRowNumber) {
    		setCurrentRowNumber(this.currentRowNumber + 1);
    	}
    	
    	return false;
    }
    
    public boolean hasPrevious() {
        return (this.currentRowNumber != 0 && this.currentRowNumber != 1);
    }
    
    public boolean previous() {
        if (hasPrevious()) {
        	setCurrentRowNumber(this.currentRowNumber - 1);
        	return true;
        }
        
        if (this.currentRowNumber == 1) {
        	setCurrentRowNumber(this.currentRowNumber - 1);
        }
        
        return false;
    }
                
    public void setBatchFetcher(BatchFetcher batchFetcher) {
        this.batchFetcher = batchFetcher;
    }
    
    public boolean absolute(int row) throws SQLException {
    	return absolute(row, 0);
    }
    
    public boolean absolute(int row, int offset) throws SQLException {
        if(row == 0) {
            setCurrentRowNumber(0);
            return false;
        }
        
        if (row > 0) {
        	
        	if (row + offset > highestRowNumber && lastRowNumber == -1) {
        		requestBatchAndWait(row + offset);
        	}
        	
        	if (row + offset <= highestRowNumber) {
        		setCurrentRowNumber(row);
        		return true;
        	}

    		setCurrentRowNumber(lastRowNumber + 1 - offset);
    		return false;
        }
        
        row -= offset;
        
        if (lastRowNumber == -1) {
    		requestBatchAndWait(Integer.MAX_VALUE);
    	}
        
        int positiveRow = lastRowNumber + row + 1;
        
        if (positiveRow <= 0) {
        	setCurrentRowNumber(0);
        	return false;
        }
        
        setCurrentRowNumber(positiveRow);
        return true;
    }
    
    public int getCurrentRowNumber() {
        return currentRowNumber;
    }
        
    private void requestBatchAndWait(int beginRow) throws SQLException{
    	setBatch(batchFetcher.requestBatch(beginRow));
	}

	void setBatch(Batch batch) {
		if (batches.size() == savedBatches) {
        	batches.remove(savedBatches - 1);
        }
		Assertion.assertTrue(batch.getLength() != 0 || batch.isLast());
		if (batch.getLastRow() != -1) {
            this.lastRowNumber = batch.getLastRow();
            this.highestRowNumber = batch.getLastRow();
        } else {
        	highestRowNumber = Math.max(batch.getEndRow(), highestRowNumber);
        }
        this.batches.add(0, batch);
	}

	public boolean hasNext() throws SQLException {
		return hasNext(1, true);
	}

	public Boolean hasNext(int next, boolean wait) throws SQLException {
    	while (this.currentRowNumber + next > highestRowNumber && lastRowNumber == -1) {
    		if (!wait) {
    			return null;
    		}
			requestNextBatch();
        }
        
        return (this.currentRowNumber + next <= highestRowNumber);
	}

	public int getFinalRowNumber() {
		return lastRowNumber;
	}

	public int getHighestRowNumber() {
		return highestRowNumber;
	}

	private void setCurrentRowNumber(int currentRowNumber) {
		if (currentRowNumber != this.currentRowNumber) {
			this.currentRow = null;
		}
		this.currentRowNumber = currentRowNumber;
	}
       
}
