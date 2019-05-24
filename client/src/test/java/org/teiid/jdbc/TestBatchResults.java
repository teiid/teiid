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

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.jdbc.BatchResults.Batch;
import org.teiid.jdbc.BatchResults.BatchFetcher;


/**
 * @since 4.3
 */
@SuppressWarnings({"nls","unchecked"})
public class TestBatchResults {

    static class MockBatchFetcher implements BatchFetcher {

        private int totalRows;
        private boolean throwException;
        private boolean useLastRow;
        List<Integer> batchCalls = new ArrayList<Integer>();

        public MockBatchFetcher() {
            this(50);
        }

        public MockBatchFetcher(int totalRows) {
            this.totalRows = totalRows;
        }

        public void setUseLastRow(boolean useLastRow) {
            this.useLastRow = useLastRow;
        }

        public Batch requestBatch(int beginRow) throws SQLException {
            batchCalls.add(beginRow);
            if (throwException) {
                throw new SQLException();
            }
            int endRow = beginRow + 9;
            if (beginRow%10==0) {
                endRow = beginRow - 9;
            }
            if (beginRow > totalRows) {
                beginRow = totalRows + 1;
                endRow = totalRows;
            } else if(beginRow > endRow) {
                if(endRow < 1) {
                    endRow = 1;
                }
                int i = beginRow;
                beginRow = endRow;
                endRow = i;
            }
            boolean last = false;
            if(endRow >= totalRows) {
                endRow = totalRows;
                last = true;
            }
            Batch batch = new Batch(createBatch(beginRow, endRow), beginRow, endRow);
            if (last || useLastRow) {
                batch.setLastRow(totalRows);
            }
            return batch;
        }

        public void throwException() {
            this.throwException = true;
        }

    }

    private static List<?>[] createBatch(int begin, int end) {
        List<Integer>[] results = new List[end - begin + 1];
        for(int i=0; i<(end - begin + 1); i++) {
            results[i] = new ArrayList<Integer>();
            results[i].add(new Integer(i+begin));
        }
        return results;
    }

    private List<?>[] createEmptyBatch() {
        return new List[0];
    }

    @Test public void testGetCurrentRow1() throws Exception{
        //empty batch
        BatchResults batchResults = getBatchResults(createEmptyBatch(), true);
        assertNull(batchResults.getCurrentRow());
        batchResults.next();
        assertNull(batchResults.getCurrentRow());
    }

    @Test public void testGetCurrentRow2() throws Exception{
        BatchResults batchResults = getBatchResults(createBatch(1, 10), false);
        assertNull(batchResults.getCurrentRow());
        batchResults.next();
        List<Integer> expectedResult = new ArrayList<Integer>();
        expectedResult.add(new Integer(1));
        assertEquals(batchResults.getCurrentRow(), expectedResult);
    }

    @Test public void testHasNext1() throws Exception{
        //empty batch
        BatchResults batchResults = getBatchResults(createEmptyBatch(), true);
        assertFalse(batchResults.hasNext());
    }

    @Test public void testHasNext2() throws Exception{
        //one row batch
        BatchResults batchResults = getBatchResults(createBatch(1, 1), false);
        assertTrue(batchResults.hasNext());
    }

    @Test public void testHasNext3() throws Exception{
        BatchResults batchResults = getBatchResults(createBatch(1, 10), false);
        assertTrue(batchResults.hasNext());
    }

    @Test public void testNext1() throws Exception{
        //empty batch
        BatchResults batchResults = getBatchResults(createEmptyBatch(), true);
        assertFalse(batchResults.next());
    }

    @Test public void testNext2() throws Exception{
        //one row batch
        BatchResults batchResults = getBatchResults(createBatch(1, 1), true);
        assertTrue(batchResults.next());
        List<Integer> expectedResult = new ArrayList<Integer>();
        expectedResult.add(new Integer(1));
        assertEquals(batchResults.getCurrentRow(), expectedResult);
        assertFalse(batchResults.next());
    }

    @Test public void testNext3() throws Exception{
        //one row batch, multiple batches
        BatchResults batchResults = getBatchResults(createBatch(1, 1), false);
        assertTrue(batchResults.next());
        assertTrue(batchResults.next());
        List<Integer> expectedResult = new ArrayList<Integer>();
        expectedResult.add(new Integer(2));
        assertEquals(batchResults.getCurrentRow(), expectedResult);
    }

    @Test public void testNext4() throws Exception{
        BatchResults batchResults = getBatchResults(createBatch(1, 10), false);
        int i;
        for(i=0; i<10; i++) {
            assertTrue(batchResults.next());
            List<Integer> expectedResult = new ArrayList<Integer>();
            expectedResult.add(new Integer(i+1));
            assertEquals(batchResults.getCurrentRow(), expectedResult);
        }
        while(batchResults.next()) {
            List<Integer> expectedResult = new ArrayList<Integer>();
            expectedResult.add(new Integer((i++)+1));
            assertEquals(batchResults.getCurrentRow(), expectedResult);
        }
        assertFalse(batchResults.next());
    }

    @Test public void testHasPrevious1() throws Exception{
        //empty batch
        BatchResults batchResults = getBatchResults(createEmptyBatch(), false);
        assertFalse(batchResults.hasPrevious());
    }

    @Test public void testHasPrevious2() throws Exception{
        //one row batch
        BatchResults batchResults = getBatchResults(createBatch(1, 1), true);
        assertFalse(batchResults.hasPrevious());
        batchResults.next();
        assertFalse(batchResults.hasPrevious());
        batchResults.next();
        assertTrue(batchResults.hasPrevious());
    }

    @Test public void testPrevious1() throws Exception{
        //empty batch
        BatchResults batchResults = getBatchResults(createEmptyBatch(), false);
        assertFalse(batchResults.previous());
    }

    @Test public void testPrevious2() throws Exception{
        //one row batch
        BatchResults batchResults = getBatchResults(createBatch(1, 1), true);
        assertTrue(batchResults.next());
        assertFalse(batchResults.previous());
        List<Integer> expectedResult = new ArrayList<Integer>();
        expectedResult.add(new Integer(1));
        while(batchResults.next()) {
        }
        assertTrue(batchResults.previous());
        assertEquals(batchResults.getCurrentRow(), expectedResult);
    }

    @Test public void testPrevious3() throws Exception{
        //one row batch, multiple batches
        BatchResults batchResults = getBatchResults(createBatch(1, 1), false);
        assertFalse(batchResults.previous());
        assertTrue(batchResults.next());
        assertFalse(batchResults.previous());
        while(batchResults.next()) {
        }
        assertTrue(batchResults.previous());
        while(batchResults.previous()) {
        }
        batchResults.next();
        batchResults.next();
        batchResults.next();
        batchResults.previous();
        List<Integer> expectedResult = new ArrayList<Integer>();
        expectedResult.add(new Integer(2));
        assertEquals(expectedResult, batchResults.getCurrentRow());
    }

    @Test public void testPrevious4() throws Exception{
        BatchResults batchResults = getBatchResults(createBatch(1, 10), false);
        int i;
        for(i=0; i<=10; i++) {
            assertTrue(batchResults.next());
        }
        for(i=10; i>0; i--) {
            batchResults.previous();
            List<Integer> expectedResult = new ArrayList<Integer>();
            expectedResult.add(new Integer(i));
            assertEquals(batchResults.getCurrentRow(), expectedResult);
        }
    }

    @Test public void testAbsolute1() throws Exception{
        //empty batch
        BatchResults batchResults = getBatchResults(createEmptyBatch(), true);
        assertFalse(batchResults.absolute(0));
        assertFalse(batchResults.absolute(1));
    }

    @Test public void testAbsolute2() throws Exception{
        //one row batch
        BatchResults batchResults = getBatchResults(createBatch(1, 1), false);
        assertFalse(batchResults.absolute(0));
        assertTrue(batchResults.absolute(1));
        assertTrue(batchResults.absolute(1));
        List<Integer> expectedResult = new ArrayList<Integer>();
        expectedResult.add(new Integer(1));
        assertEquals(batchResults.getCurrentRow(), expectedResult);
    }

    @Test public void testAbsolute3() throws Exception{
        BatchResults batchResults = getBatchResults(createBatch(1, 10), false);
        batchResults.setBatchFetcher(new MockBatchFetcher(200));
        assertFalse(batchResults.absolute(0));
        assertTrue(batchResults.absolute(11));
        List<Integer> expectedResult = new ArrayList<Integer>();
        expectedResult.add(new Integer(11));
        assertEquals(batchResults.getCurrentRow(), expectedResult);
        assertTrue(batchResults.absolute(1));
        expectedResult = new ArrayList<Integer>();
        expectedResult.add(new Integer(1));
        assertEquals(batchResults.getCurrentRow(), expectedResult);
        assertTrue(batchResults.absolute(100));
        expectedResult = new ArrayList<Integer>();
        expectedResult.add(new Integer(100));
        assertEquals(batchResults.getCurrentRow(), expectedResult);
    }

    //move backwards with absolute
    @Test public void testAbsolute4() throws Exception{
        //one row batch
        BatchResults batchResults = getBatchResults(createBatch(1, 1), false);
        assertTrue(batchResults.absolute(10));
        assertTrue(batchResults.absolute(2));
        List<Integer> expectedResult = new ArrayList<Integer>();
        expectedResult.add(new Integer(2));
        assertEquals(batchResults.getCurrentRow(), expectedResult);
    }

    @Test public void testAbsolute5() throws Exception{
        //one row batch
        BatchResults batchResults = getBatchResults(createBatch(1, 1), false);
        assertTrue(batchResults.absolute(-1));
        List<Integer> expectedResult = new ArrayList<Integer>();
        expectedResult.add(new Integer(50));
        assertEquals(expectedResult, batchResults.getCurrentRow());

        assertFalse(batchResults.absolute(-100));
    }

    @Test public void testAbsoluteWithLastRow() throws Exception{
        Batch batch = new Batch(createBatch(1, 10), 1, 10);
        batch.setLastRow(50);
        MockBatchFetcher mbf = new MockBatchFetcher();
        mbf.setUseLastRow(true);
        BatchResults batchResults = new BatchResults(mbf, batch, BatchResults.DEFAULT_SAVED_BATCHES);
        assertTrue(batchResults.absolute(41));
        assertEquals(Arrays.asList(41), batchResults.getCurrentRow());
        //check to ensure that we skipped all the other batches
        assertEquals(Arrays.asList(41), mbf.batchCalls);
    }

    @Test public void testCurrentRowNumber() throws Exception {
        BatchResults batchResults = getBatchResults(createBatch(1, 1), true);
        assertEquals(0, batchResults.getCurrentRowNumber());
        batchResults.next();
        assertEquals(1, batchResults.getCurrentRowNumber());
        batchResults.next();
        assertEquals(2, batchResults.getCurrentRowNumber());
        assertFalse(batchResults.next());
        assertEquals(2, batchResults.getCurrentRowNumber());
    }

    @Test(expected=SQLException.class) public void testSetException() throws Exception {
        BatchResults batchResults = getBatchResults(createBatch(1, 1), false);
        MockBatchFetcher batchFetcher = new MockBatchFetcher();
        batchResults.setBatchFetcher(batchFetcher);
        batchFetcher.throwException();
        batchResults.next();
        batchResults.hasNext();
    }

    BatchResults getBatchResults(List<?>[] batch, boolean isLast) {
        Batch batch2 = new Batch(batch, 1, batch.length);
        if (isLast) {
            batch2.setLastRow(batch.length);
        }
        BatchResults results = new BatchResults(null, batch2, BatchResults.DEFAULT_SAVED_BATCHES);
        if (!isLast) {
            results.setBatchFetcher(new MockBatchFetcher());
        }
        return results;
    }

    @Test public void testBatching() throws Exception {
        BatchResults batchResults = getBatchResults(createBatch(1, 10), false);
        MockBatchFetcher batchFetcher = new MockBatchFetcher(60);
        batchResults.setBatchFetcher(batchFetcher);
        for(int i=0; i<45; i++) {
            assertTrue(batchResults.next());
        }

        for(int i=0; i<44; i++) {
            assertTrue(batchResults.previous());
            assertEquals(new Integer(44 - i), batchResults.getCurrentRow().get(0));
        }

        // verify batch calls
        checkResults(new int[] {
            // going forwards - end > begin
            11,
            21,
            31,
            41,
            // going backwards - begin > end
            // last 3 batches were saved, only need the first 2 again
            20,
            10,
        }, batchFetcher.batchCalls);

        assertTrue(batchResults.absolute(50));
        assertEquals(new Integer(50), batchResults.getCurrentRow().get(0));
    }

    private void checkResults(int[] expectedCalls, List<Integer> batchCalls) {
        assertEquals(expectedCalls.length, batchCalls.size());

        for(int i=0; i<batchCalls.size(); i++) {
            int range = batchCalls.get(i);
            int expected = expectedCalls[i];
            assertEquals("On call " + i + " expected different begin", expected, range);
        }
    }

}
