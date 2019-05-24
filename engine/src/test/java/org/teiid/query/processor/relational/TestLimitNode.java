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

package org.teiid.query.processor.relational;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.query.sql.symbol.Constant;

public class TestLimitNode {

    @Test public void testLimitInFirstBatch() throws Exception {
        LimitNode node = getLimitNode(40, new FakeRelationalNode(2, getRows(100), 50));

        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(40, batch.getRowCount());
        assertEquals(1, batch.getBeginRow());
        assertEquals(40, batch.getEndRow());
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testLimitAtBatchSize() throws Exception {
        LimitNode node = getLimitNode(50, new FakeRelationalNode(2, getRows(100), 50));

        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(50, batch.getRowCount());
        assertEquals(1, batch.getBeginRow());
        assertEquals(50, batch.getEndRow());
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testLimitInSecondBatch() throws Exception {
        LimitNode node = getLimitNode(55, new FakeRelationalNode(2, getRows(100), 50));

        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(50, batch.getRowCount());
        assertEquals(1, batch.getBeginRow());
        assertEquals(50, batch.getEndRow());
        assertFalse(batch.getTerminationFlag());

        batch = node.nextBatch();
        assertEquals(5, batch.getRowCount());
        assertEquals(51, batch.getBeginRow());
        assertEquals(55, batch.getEndRow());
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testLimitMultipleOfBatchSize() throws Exception {
        LimitNode node = getLimitNode(100, new FakeRelationalNode(2, getRows(150), 50));

        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(50, batch.getRowCount());
        assertEquals(1, batch.getBeginRow());
        assertEquals(50, batch.getEndRow());
        assertFalse(batch.getTerminationFlag());

        batch = node.nextBatch();
        assertEquals(50, batch.getRowCount());
        assertEquals(51, batch.getBeginRow());
        assertEquals(100, batch.getEndRow());
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testLimitProducesMultipleBatches() throws Exception {
        LimitNode node = getLimitNode(130, new FakeRelationalNode(2, getRows(300), 50));

        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(50, batch.getRowCount());
        assertEquals(1, batch.getBeginRow());
        assertEquals(50, batch.getEndRow());
        assertFalse(batch.getTerminationFlag());

        batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(50, batch.getRowCount());
        assertEquals(51, batch.getBeginRow());
        assertEquals(100, batch.getEndRow());
        assertFalse(batch.getTerminationFlag());

        batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(30, batch.getRowCount());
        assertEquals(101, batch.getBeginRow());
        assertEquals(130, batch.getEndRow());
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testLimitGetsNoRows() throws Exception {
        LimitNode node = getLimitNode(100, new FakeRelationalNode(2, getRows(0), 50));

        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(0, batch.getRowCount());
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testZeroLimit() throws Exception {
        LimitNode node = getLimitNode(0, new FakeRelationalNode(2, getRows(100), 50));

        TupleBatch batch = node.nextBatch();

        batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(0, batch.getRowCount());
        assertEquals(1, batch.getBeginRow());
        assertEquals(0, batch.getEndRow());
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testOffsetInFirstBatch() throws Exception {
        LimitNode node = getOffsetNode(49, new FakeRelationalNode(2, getRows(100), 50));
        // batch 1
        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(1, batch.getRowCount());
        assertEquals(1, batch.getBeginRow());
        assertEquals(1, batch.getEndRow());
        assertEquals(Arrays.asList(new Object[] {new Integer(50)}), batch.getTuple(1));
        assertFalse(batch.getTerminationFlag());
        // batch2
        batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(50, batch.getRowCount());
        assertEquals(2, batch.getBeginRow());
        assertEquals(51, batch.getEndRow());
        assertEquals(Arrays.asList(new Object[] {new Integer(51)}), batch.getTuple(2));
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testOffsetAtBatchSize() throws Exception {
        LimitNode node = getOffsetNode(50, new FakeRelationalNode(2, getRows(100), 50));

        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(50, batch.getRowCount());
        assertEquals(1, batch.getBeginRow());
        assertEquals(50, batch.getEndRow());
        assertEquals(Arrays.asList(new Object[] {new Integer(51)}), batch.getTuple(1));
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testOffsetInSecondBatch() throws Exception {
        LimitNode node = getOffsetNode(55, new FakeRelationalNode(2, getRows(100), 50));
        // batch 1
        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(45, batch.getRowCount());
        assertEquals(1, batch.getBeginRow());
        assertEquals(45, batch.getEndRow());
        assertEquals(Arrays.asList(new Object[] {new Integer(56)}), batch.getTuple(1));
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testOffsetMultipleOfBatchSize() throws Exception {
        LimitNode node = getOffsetNode(100, new FakeRelationalNode(2, getRows(300), 50));

        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(50, batch.getRowCount());
        assertEquals(1, batch.getBeginRow());
        assertEquals(50, batch.getEndRow());
        assertEquals(Arrays.asList(new Object[] {new Integer(101)}), batch.getTuple(1));
        assertFalse(batch.getTerminationFlag());
    }

    @Test public void testOffsetGreaterThanRowCount() throws Exception {
        LimitNode node = getOffsetNode(100, new FakeRelationalNode(2, getRows(10), 50));

        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(0, batch.getRowCount());
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testOffsetNoRows() throws Exception {
        LimitNode node = getOffsetNode(100, new FakeRelationalNode(2, getRows(0), 50));

        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(0, batch.getRowCount());
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testZeroOffset() throws Exception {
        LimitNode node = getOffsetNode(0, new FakeRelationalNode(2, getRows(100), 50));

        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(50, batch.getRowCount());
        assertEquals(1, batch.getBeginRow());
        assertEquals(50, batch.getEndRow());
        assertFalse(batch.getTerminationFlag());

        batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(50, batch.getRowCount());
        assertEquals(51, batch.getBeginRow());
        assertEquals(100, batch.getEndRow());
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testOffsetWithoutLimit() throws Exception {
        LimitNode node = new LimitNode(1, null, new Constant(new Integer(10)));
        node.addChild(new FakeRelationalNode(2, getRows(10), 50));
        node.open();

        TupleBatch batch = node.nextBatch();
        assertNotNull(batch);
        assertEquals(0, batch.getRowCount());
        assertTrue(batch.getTerminationFlag());
    }

    static List<?>[] getRows(int rows) {
        List<?>[] data = new List[rows];
        for (int i = 0; i < rows; i++) {
            data[i] = Arrays.asList(new Object[] {new Integer(i+1)});
        }
        return data;
    }

    private static LimitNode getOffsetNode(int offset, RelationalNode child) throws Exception {
        LimitNode node = new LimitNode(1, null, new Constant(new Integer(offset)));
        node.addChild(child);
        node.open();
        return node;
    }

    private static LimitNode getLimitNode(int limit, RelationalNode child) throws Exception {
        LimitNode node = new LimitNode(1, new Constant(new Integer(limit)), new Constant(new Integer(0)));
        node.addChild(child);
        node.open();
        return node;
    }

    @Test public void testClone() {
        LimitNode node = new LimitNode(1, new Constant(new Integer(-1)), null);

        LimitNode clone = (LimitNode)node.clone();

        assertEquals(node.getLimitExpr(), clone.getLimitExpr());
        assertNull(clone.getOffsetExpr());

        node = new LimitNode(1, null, new Constant(new Integer(-1)));
        clone = (LimitNode)node.clone();

        assertNull(clone.getLimitExpr());
    }
}
