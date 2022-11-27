/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * @since 4.3
 */
public class BatchResults {

    public interface BatchFetcher {
        Batch requestBatch(int beginRow) throws SQLException;
    }

    static class Batch{
        private List<?>[] batch;
        private int beginRow;
        private int endRow;
        private boolean isLast;
        private int lastRow = -1;

        Batch(List<?>[] batch, int beginRow, int endRow){
            this.batch = batch;
            this.beginRow = beginRow;
            this.endRow = this.beginRow + this.batch.length - 1;
            if (endRow != this.endRow) {
                this.isLast = true;
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

        List<?> getRow(int index) {
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
    private boolean tailLast;

    public BatchResults(BatchFetcher batchFetcher, Batch batch, int savedBatches) {
        this.batchFetcher = batchFetcher;
        this.savedBatches = savedBatches;
        this.setBatch(batch);
    }

    /**
     * Moving forward through the results it's expected that the batches are arbitrarily size.
     * Moving backward through the results it's expected that the batches will match the fetch size.
     */
    public List<?> getCurrentRow() throws SQLException {
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
            setCurrentRow(batch);
            return currentRow;
        }
        requestBatchAndWait(this.currentRowNumber);
        Batch batch = batches.get(0);
        setCurrentRow(batch);
        return currentRow;
    }

    private void setCurrentRow(Batch batch) {
        currentRow = batch.getRow(this.currentRowNumber);
        if (batch.isLast() && batch.getEndRow() == this.currentRowNumber) {
            currentRow = null;
        }
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
        if (batch.getLastRow() != -1) {
            this.lastRowNumber = batch.getLastRow();
            this.highestRowNumber = batch.getLastRow();
        } else {
            highestRowNumber = Math.max(batch.getEndRow(), highestRowNumber);
            tailLast = batch.isLast();
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
        boolean result = this.currentRowNumber + next <= highestRowNumber;
        if (result && !wait) {
            for (int i = 0; i < batches.size(); i++) {
                Batch batch = batches.get(i);
                if (this.currentRowNumber + next < batch.getBeginRow()) {
                    continue;
                }
                if (this.currentRowNumber + next> batch.getEndRow()) {
                    continue;
                }
                return Boolean.TRUE;
            }
            return null; //needs to be fetched
        }
        return result;
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

    public boolean isTailLast() {
        return tailLast;
    }

}
