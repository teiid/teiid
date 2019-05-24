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

package org.teiid.common.buffer;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import javax.sql.rowset.serial.SerialClob;

import org.junit.Test;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBuffer.TupleBufferTupleSource;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.symbol.ElementSymbol;

public class TestTupleBuffer {

    @Test public void testForwardOnly() throws Exception {
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        List<ElementSymbol> schema = Arrays.asList(x);
        TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tb.setForwardOnly(true);
        tb.addTuple(Arrays.asList(1));
        TupleBatch batch = tb.getBatch(1);
        assertTrue(!batch.getTerminationFlag());
        assertEquals(1, batch.getBeginRow());
        try {
            tb.getBatch(1);
            fail("expected exception"); //$NON-NLS-1$
        } catch (AssertionError e) {

        }
        tb.addTuple(Arrays.asList(1));
        tb.close();
        batch = tb.getBatch(2);
        assertTrue(batch.getTerminationFlag());
        assertEquals(2, batch.getBeginRow());
    }

    @Test public void testReverseIteration() throws Exception {
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        List<ElementSymbol> schema = Arrays.asList(x);
        TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tb.addTuple(Arrays.asList(1));
        tb.addTuple(Arrays.asList(2));
        TupleBufferTupleSource tbts = tb.createIndexedTupleSource();
        tbts.setReverse(true);
        assertTrue(tbts.hasNext());
        assertEquals(2, tbts.nextTuple().get(0));
        assertEquals(1, tbts.nextTuple().get(0));
        assertFalse(tbts.hasNext());
    }

    @Test public void testTruncate() throws Exception {
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        List<ElementSymbol> schema = Arrays.asList(x);
        TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tb.setBatchSize(2);
        for (int i = 0; i < 5; i++) {
            tb.addTuple(Arrays.asList(1));
        }
        TupleBatch batch = tb.getBatch(1);
        assertTrue(!batch.getTerminationFlag());
        assertEquals(2, batch.getEndRow());
        tb.close();
        assertEquals(5, tb.getManagedRowCount());
        tb.truncateTo(3);
        assertEquals(3, tb.getManagedRowCount());
        assertEquals(3, tb.getRowCount());
        batch = tb.getBatch(3);
        assertTrue(batch.getTerminationFlag());
        tb.truncateTo(2);
        assertEquals(2, tb.getManagedRowCount());
        assertEquals(2, tb.getRowCount());
        batch = tb.getBatch(2);
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testTruncatePartial() throws Exception {
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        List<ElementSymbol> schema = Arrays.asList(x);
        TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tb.setBatchSize(64);
        for (int i = 0; i < 65; i++) {
            tb.addTuple(Arrays.asList(1));
        }
        TupleBatch batch = tb.getBatch(1);
        assertTrue(!batch.getTerminationFlag());
        assertEquals(65, tb.getManagedRowCount());
        tb.truncateTo(3);
        assertEquals(3, tb.getManagedRowCount());
        assertEquals(3, tb.getRowCount());
        batch = tb.getBatch(3);
    }

    @Test public void testTruncatePartial1() throws Exception {
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        List<ElementSymbol> schema = Arrays.asList(x);
        TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tb.setBatchSize(128);
        for (int i = 0; i < 131; i++) {
            tb.addTuple(Arrays.asList(1));
        }
        tb.truncateTo(129);
        assertEquals(129, tb.getManagedRowCount());
        assertEquals(129, tb.getRowCount());
    }

    @Test public void testTruncateMultiple() throws Exception {
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        List<ElementSymbol> schema = Arrays.asList(x);
        TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tb.setBatchSize(16);
        for (int i = 0; i < 131; i++) {
            tb.addTuple(Arrays.asList(1));
        }
        tb.truncateTo(17);
        assertEquals(17, tb.getManagedRowCount());
        assertEquals(17, tb.getRowCount());
    }

    @Test public void testLobHandling() throws Exception {
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        x.setType(DataTypeManager.DefaultDataClasses.CLOB);
        List<ElementSymbol> schema = Arrays.asList(x);
        TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tb.setInlineLobs(false);
        ClobType c = new ClobType(new SerialClob(new char[0]));
        TupleBatch batch = new TupleBatch(1, new List[] {Arrays.asList(c)});
        tb.addTupleBatch(batch, false);
        assertNotNull(tb.getLobReference(c.getReferenceStreamId()));
    }

}
