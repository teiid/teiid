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
import static org.mockito.Mockito.*;

import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.junit.Test;
import org.mockito.MockSettings;
import org.mockito.Mockito;
import org.teiid.client.DQP;
import org.teiid.client.ResultsMessage;
import org.teiid.client.lob.LobChunk;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.XMLType;

@SuppressWarnings("nls")
public class TestResultSet {

    private static final int BATCH_SIZE = 400;

    /** test next() without walking through */
    @Test public void testNext1() throws SQLException {
        ResultSet cs =  helpExecuteQuery();
        assertEquals(" Actual doesn't match with expected. ", new Integer(0), new Integer(cs.getRow())); //$NON-NLS-1$
        cs.close();
    }

    /** test next() with walking through all the rows and compare records */
    @Test public void testNext2() throws SQLException {
        List<?>[] expected = TestAllResultsImpl.exampleResults1(1000);
        ResultSetImpl cs =  helpExecuteQuery();

        int i=0;
        while(cs.next()) {
           assertEquals(" Actual doesn't match with expected. ", expected[i], cs.getCurrentRecord()); //$NON-NLS-1$
           assertEquals((i < 800?BATCH_SIZE:200) - (i%BATCH_SIZE) - 1, cs.available());
           i++;
        }

        cs.close();
    }

    /** test with LargeA -- only work with real model rather than fake metadata*/

    // Note for all the following: processor batch size is 100,
    // so some of these tests check what happens when the client
    // fetch size is above, the same, or below it
    public static final int PROC_BATCH_SIZE = 100;

    /** Test stability when next() is called beyond the rowcount. */
    @Test public void testNextBeyondEnd_fetchEqualsCount() throws Exception {
        helpTestNextBeyondResultSet(1000, 1000);
    }

    /** Test stability when next() is called beyond the rowcount. */
    @Test public void testNextBeyondEnd_fetchLessThanCount() throws Exception {
        helpTestNextBeyondResultSet(100, 1000);
    }

    /** Test stability when next() is called beyond the rowcount with one more row. */
    @Test public void testNextBeyondEnd_fetchLessThanCount1() throws Exception {
        helpTestNextBeyondResultSet(100, 101);
    }

    /** Test stability when next() is called beyond the rowcount. */
    @Test public void testNextBeyondEnd_fetchLessThanCountNonMultiple() throws Exception {
        helpTestNextBeyondResultSet(120, 1000);
    }

    /** Test stability when next() is called beyond the rowcount. */
    @Test public void testNextBeyondEnd_fetchGreaterThanCount() throws Exception {
        helpTestNextBeyondResultSet(300, PROC_BATCH_SIZE);
    }

    /** Test stability when next() is called beyond the rowcount. */
    @Test public void testNextBeyondEnd_fetchGreaterThanCountNonMultiple() throws Exception {
        helpTestNextBeyondResultSet(310, PROC_BATCH_SIZE-50);
    }

    /** Test stability when next() is called beyond the rowcount. */
    @Test public void testNextBeyondEnd_fetchGreaterThanCountNonMultiple2() throws Exception {
        helpTestNextBeyondResultSet(300, PROC_BATCH_SIZE+10);
    }

    /** Test that the returned results walks through all results if
     * fetchSize &lt; rows &lt; proc batch size.
     * Test for defect 11356
     */
    @Test public void testNextBeyondEnd_fetchLessThanCount_ResultsBetweenFetchAndProcBatch() throws Exception {
        helpTestNextBeyondResultSet(30, PROC_BATCH_SIZE-25);
    }

    public void helpTestNextBeyondResultSet(int fetchSize, int numRows) throws Exception {
        ResultSet cs = helpExecuteQuery(fetchSize, numRows, ResultSet.TYPE_SCROLL_INSENSITIVE);
        try {
            Object lastRowValue = null;
            for (int rowNum = 1; rowNum <= numRows; rowNum++) {
                assertEquals("Should return true before end cs.next()", true, cs.next()); //$NON-NLS-1$
            }

            lastRowValue = cs.getObject(1);

            // Should just return false and leave cursor where it is
            for(int i=numRows+1; i<numRows+4; i++) {
                assertEquals("Should return false when going past the end: " + i, false, cs.next()); //$NON-NLS-1$
                assertEquals("Is after last should be true: " + i, true, cs.isAfterLast()); //$NON-NLS-1$
            }

            // Should still be just after last row
            cs.previous();
            assertEquals("Is last should be true", true, cs.isLast()); //$NON-NLS-1$
            assertEquals("Not on last row", lastRowValue, cs.getObject(1));              //$NON-NLS-1$

        } finally {
            cs.close();
        }
    }

    /** test both next() and previous() -- when result set scroll in bidirection */
    @Test public void testBidirection() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        assertNotNull(cs);
        cs.absolute(290);
        assertEquals(" Actual value doesn't match with expected one.", new Integer(290), cs.getCurrentRecord().get(0)); //$NON-NLS-1$
        cs.next();
        assertEquals(" Actual value doesn't match with expected one.", new Integer(291), cs.getCurrentRecord().get(0)); //$NON-NLS-1$
        cs.next();
        assertEquals(" Actual value doesn't match with expected one.", new Integer(292), cs.getCurrentRecord().get(0)); //$NON-NLS-1$
        cs.previous();
        assertEquals(" Actual value doesn't match with expected one.", new Integer(291), cs.getCurrentRecord().get(0)); //$NON-NLS-1$
        cs.next();
        assertEquals(" Actual value doesn't match with expected one.", new Integer(292), cs.getCurrentRecord().get(0)); //$NON-NLS-1$
        cs.close();
    }

    /** test hasNext() without walking through any row */
    @Test public void testHasNext1() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        assertEquals(" hasNext() doesn't match expected value. ", true, cs.hasNext()); //$NON-NLS-1$
        cs.close();
    }

    /** test hasNext() with blocking for the Next batch -- triggering point */
    @Test public void testHasNext2() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        cs.absolute(100);
        assertEquals(" hasNext() doesn't match expected value. ", true, cs.hasNext());  //$NON-NLS-1$
        cs.close();
    }

    /** test hasNext() with nextBatch!=null -- short response */
    @Test public void testHasNext3() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        int i = 0;
        while (cs.next()) {
            if (i == 289) {
                break;
            }
            i++;
        }
        assertEquals(" hasNext() doesn't match expected value. ", true, cs.hasNext()); //$NON-NLS-1$
        cs.close();
    }

    /** at the end of all batches */
    @Test public void testHasNext4() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        cs.absolute(1000);
        assertTrue(!cs.hasNext());
        cs.close();
    }

    /** walk all way through from the end back to first row */
    @Test public void testPrevious1() throws SQLException {
        ResultSetImpl cs = helpExecuteQuery();
        List<?>[] expected = TestAllResultsImpl.exampleResults1(1000);
        while(cs.next()) {
            //System.out.println(" rs.next == " + cs.getCurrentRecord());
        }
        // cursor is after the last row. getRow() should return 0 when not on a valid row
        assertEquals(" current row doesn't match expected value. ", 0, cs.getRow()); //$NON-NLS-1$

        int i= 1000;
        while (cs.previous()) {
            //System.out.println(" rs.previous == " + cs.getCurrentRecord());
            assertEquals(" Actual doesn't match with expected. ", expected[i-1], cs.getCurrentRecord()); //$NON-NLS-1$
            i--;
        }
        assertEquals(" current row doesn't match expected value. ", 0, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** test the previous in the middle of a batch */
    @Test public void testPrevious2() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        cs.absolute(290);

        // cursor is at the row of 289 now
        assertTrue(cs.previous());
        assertEquals(" current row doesn't match expected value. ", 289, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** walk all way through from the end back to first row */
    @Test public void testPrevious3() throws Exception {
        //large batch size
        ResultSetImpl cs = helpExecuteQuery(600, 10000, ResultSet.TYPE_SCROLL_INSENSITIVE);
        List<?>[] expected = TestAllResultsImpl.exampleResults1(10000);
        while(cs.next()) {
        }
        // cursor is after the last row. getRow() should return 0 when not on a valid row
        assertEquals(" current row doesn't match expected value. ", 0, cs.getRow()); //$NON-NLS-1$

        int i= 10000;
        while (cs.previous()) {
            //System.out.println(" rs.previous == " + cs.getCurrentRecord());
            assertEquals(" Actual doesn't match with expected. ", expected[i-1], cs.getCurrentRecord()); //$NON-NLS-1$
            i--;
        }
        assertEquals(" current row doesn't match expected value. ", 0, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** walk all way through from the end back to first row */
    @Test public void testPrevious4() throws Exception {
        //small batch size
        ResultSetImpl cs = helpExecuteQuery(50, 1000, ResultSet.TYPE_SCROLL_INSENSITIVE);
        List<?>[] expected = TestAllResultsImpl.exampleResults1(1000);
        while(cs.next()) {
            //System.out.println(" rs.next == " + cs.getCurrentRecord());
        }
        // cursor is after the last row. getRow() should return 0 when not on a valid row
        assertEquals(" current row doesn't match expected value. ", 0, cs.getRow()); //$NON-NLS-1$

        int i= 1000;
        while (cs.previous()) {
            //System.out.println(" rs.previous == " + cs.getCurrentRecord());
            assertEquals(" Actual doesn't match with expected. ", expected[i-1], cs.getCurrentRecord()); //$NON-NLS-1$
            i--;
        }
        assertEquals(" current row doesn't match expected value. ", 0, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** test rare case that cursor change direction */
    @Test public void testChangeDirection() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        cs.absolute(291);
        cs.previous();

        assertEquals(" current row doesn't match expected value. ", 290, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    @Test public void testIsFirst() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        cs.next();
        assertTrue(cs.isFirst());
        cs.close();
    }

    /** test cursor is in the middle of all batches */
    @Test public void testIsLast1() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        cs.next();
        assertTrue(!cs.isLast());
        cs.close();
    }

    /** test cursor at the triggering point -- blocking case*/
    @Test public void testIsLast2() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();

        int i = 0;
        while (cs.next()) {
            if (i == 99) {
                break;
            }
            i++;
        }

        assertTrue(!cs.isLast());
        cs.close();
    }

    /** test cursor at the last row of all batches */
    @Test public void testIsLast3() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        cs.absolute(1000);
        assertTrue(cs.isLast());
        cs.close();
    }

    @Test public void testIsBeforeFirst() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        assertTrue(cs.isBeforeFirst());
        cs.close();
    }

    @Test public void testBeforeFirst() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();

        // move to row 1
        cs.next();
        assertEquals(" Current row number doesn't match with expected one.", 1, cs.getRow()); //$NON-NLS-1$

        // move back to before first row
        cs.beforeFirst();
        assertTrue(cs.isBeforeFirst());
        cs.close();
    }

    @Test public void testFirst() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();

        // move to row #2
        cs.next();
        cs.next();
        assertEquals(" Current row number doesn't match with expected one.", 2, cs.getRow()); //$NON-NLS-1$

        // move back to the 1st row
        cs.first();
        assertEquals(" Current row number doesn't match with expected one.", 1, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    @Test public void testAfterLast() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        cs.afterLast();
        assertTrue(cs.isAfterLast());
        cs.close();
    }

    /** right after the last row */
    @Test public void testIsAfterLast1() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        cs.absolute(1000);
        cs.next();
        assertTrue(cs.isAfterLast());
        cs.close();
    }

    /** right before the first */
    @Test public void testIsAfterLast2() throws Exception {
        ResultSetImpl cs =  helpExecuteQuery();
        assertTrue(!cs.isAfterLast());
        cs.close();
    }

    /** absolute with cursor movement backward in the same batch -- absolute(positive) */
    @Test public void testAbsolute1() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();

        // move to row #2
        cs.next();
        cs.next();
        assertEquals(" Current row number doesn't match with expected one.", 2, cs.getRow()); //$NON-NLS-1$

        // move back to the 1st row
        cs.absolute(1);
        assertEquals(" Current row number doesn't match with expected one.", 1, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** absolute with cursor movement forward in the same batch -- absolute(positive) */
    @Test public void testAbsolute2() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();

        // move to row #2
        cs.next();
        cs.next();
        assertEquals(" Current row number doesn't match with expected one.", 2, cs.getRow()); //$NON-NLS-1$

        // move back to the 1st row
        cs.absolute(3);
        assertEquals(" Current row number doesn't match with expected one.", 3, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** absolute with cursor movement forward -- absolute(positive) -- blocking */
    @Test public void testAbsolute3() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();

        // move to row #2
        cs.next();
        cs.next();
        assertEquals(" Current row number doesn't match with expected one.", 2, cs.getRow()); //$NON-NLS-1$

        // move to row #100 -- blocking
        cs.absolute(100);
        assertEquals(" Current row number doesn't match with expected one.", 100, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** absolute with cursor movement forward -- absolute(positive) -- triggering point */
    @Test public void testAbsolute4() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();

        // move to row #2
        cs.next();
        cs.next();
        assertEquals(" Current row number doesn't match with expected one.", 2, cs.getRow()); //$NON-NLS-1$

        // move to row #200 -- new batch
        cs.absolute(200);
        assertEquals(" Current row number doesn't match with expected one.", 200, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** absolute with cursor movement back in the same batch -- absolute(negative) */
    @Test public void testAbsolute5() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();

        // move to row #2
        cs.next();
        cs.next();
        assertEquals(" Current row number doesn't match with expected one.", 2, cs.getRow()); //$NON-NLS-1$

        // move back to the 1st row
        cs.absolute(-1);
        assertEquals(" Current row number doesn't match with expected one.", 1000, cs.getRow());           //$NON-NLS-1$
        cs.close();
    }

    /** absolute after last row */
    @Test public void testAbsolute6() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        cs.absolute(1005);
        // Cursor should be after last row. getRow() should return 0 because
        // cursor is not on a valid row
        assertEquals(" Current row number doesn't match with expected one.", 0, cs.getRow());           //$NON-NLS-1$
        cs.close();
    }

    /** relative(positive) -- forward to another batch */
    @Test public void testRelative1() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();

        // move to the row #3
        cs.absolute(3);
        assertEquals(" Current row number doesn't match with expected one.", 3, cs.getRow()); //$NON-NLS-1$

        // move to the row #140
        cs.relative(137);
        assertEquals(" Current row number doesn't match with expected one.", 140, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** relative(negative) -- backward to another batch */
    @Test public void testRelative2() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();

        // move to the row #137
        cs.absolute(137);
        assertEquals(" Current row number doesn't match with expected one.", 137, cs.getRow()); //$NON-NLS-1$

        // move to the row #4
        cs.relative(-133);
        assertEquals(" Current row number doesn't match with expected one.", 4, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** relative(negative) -- backward to triggering point or blocking batch */
    @Test public void testRelative3() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();

        // move to the row #137
        cs.absolute(137);
        assertEquals(" Current row number doesn't match with expected one.", 137, cs.getRow()); //$NON-NLS-1$

        // move to the row #100
        cs.relative(-37);
        assertEquals(" Current row number doesn't match with expected one.", 100, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** relative(negative) -- backward to triggering point or blocking batch */
    @Test public void testRelative4() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();

        // move to the row #237 in the third batch, so that the fourth batch has been requested when we switch direction
        cs.absolute(237);
        assertEquals(" Current row number doesn't match with expected one.", 237, cs.getRow()); //$NON-NLS-1$

        // move to the row #37
        cs.relative(-200);
        assertEquals(" Current row number doesn't match with expected one.", 37, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** in the first fetched batch */
    @Test public void testGetRow1() throws SQLException {
        ResultSet cs =  helpExecuteQuery();

        int i = 0;
        while (cs.next()) {
            if (i == 102) {
                break;
            }
            i++;
        }

        assertEquals(" Current row number doesn't match with expected one.", i+1, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** in the first batch */
    @Test public void testGetRow2() throws SQLException {
        ResultSet cs =  helpExecuteQuery();

        cs.next();
        assertEquals(" Current row number doesn't match with expected one.", 1, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** in the triggering point -- blocking  */
    @Test public void testGetRow3() throws SQLException {
        ResultSet cs =  helpExecuteQuery();
        int i = 0;
        while (cs.next()) {
            if (i == 99) {
                break;
            }
            i++;
        }
        assertEquals(" Current row number doesn't match with expected one.", 100, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    @Test public void testGetCurrentRecord() throws SQLException {
        ResultSet cs =  helpExecuteQuery();
        cs.absolute(103);
        assertEquals(" Current record doesn't match with expected one.", new Integer(103), ((ResultSetImpl)cs).getCurrentRecord().get(0));                 //$NON-NLS-1$
        cs.close();
    }

    /** test close() without walking through any of the record*/
    @Test public void testClose() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        assertEquals(" Actual doesn't match with expected. ", new Integer(0), new Integer(cs.getRow())); //$NON-NLS-1$
        cs.close();
    }

    /** test basic results-related metadata */
    @Test public void testGetMetaData() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();

        // check result set metadata
        // expected column info.
        List<String> columnName = getBQTRSMetaData1a();
        List<Integer> columnType = getBQTRSMetaData1b();
        List<String> columnTypeName = getBQTRSMetaData1c();

        ResultSetMetaData rm = cs.getMetaData();
        assertNotNull(rm);

        for (int j = 1; j <= rm.getColumnCount(); j++) {
            assertEquals(" Actual doesn't match with expected. ", columnName.get(j-1), rm.getColumnLabel(j)); //$NON-NLS-1$
            assertEquals(" Actual doesn't match with expected. ", columnType.get(j-1), new Integer(rm.getColumnType(j))); //$NON-NLS-1$
            assertEquals(" Actual doesn't match with expected. ", columnTypeName.get(j-1), rm.getColumnTypeName(j)); //$NON-NLS-1$
        }

        cs.close();
    }

    @Test public void testFindColumn() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        ResultSetMetaData rm = cs.getMetaData();
        assertNotNull(rm);
        //assertEquals(" Actual columnName doesn't match with expected. ", 1, cs.findColumn("BQT1.MediumA.IntKey"));
        assertEquals(" Actual columnName doesn't match with expected. ", 1, cs.findColumn("IntKey"));                       //$NON-NLS-1$ //$NON-NLS-2$
        cs.close();
    }

    @Test public void testFindNonExistentColumn() throws SQLException {
        ResultSet rs = helpExecuteQuery();
        rs.next();
        try {
            rs.findColumn("BOGUS"); //$NON-NLS-1$
        } catch(SQLException e) {
        }

        try {
            rs.getObject("BOGUS"); //$NON-NLS-1$
        } catch(SQLException e) {
        }
        rs.close();
    }

    @Test public void testGetStatement() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        assertNotNull(cs.getStatement());
        cs.close();
    }

    @Test public void testGetPlanDescription() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        assertNotNull(cs);

        assertNull((cs.getStatement()).getPlanDescription());
        cs.close();
    }

    /** getObject(String) */
    @Test public void testGetObject2() throws SQLException {
        ResultSet cs =  helpExecuteQuery();

        // move to the 1st row
        cs.next();
        assertEquals(" Actual value doesn't match with expected. ", new Integer(1), cs.getObject("IntKey")); //$NON-NLS-1$ //$NON-NLS-2$
        cs.close();
    }

    @Test public void testGetWarnings() throws SQLException {
        ResultSet cs =  helpExecuteQuery();
        assertNull(cs.getWarnings());
        cs.close();
    }

    @Test public void testGetCursorName() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        assertNull(cs.getCursorName());
        cs.close();
    }

    @Test public void testAllGetters() throws SQLException {
        ResultSetImpl cs =  helpExecuteQuery();
        cs.next();
        assertEquals(" Actual value of getInt() doesn't match with expected one. ", 1, cs.getInt("IntKey")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(" Actual value of getString() doesn't match with expected one. ", "1", cs.getString("IntKey")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        // Add these back when the MediumA has all those columns
        assertEquals(" Actual value of getFloat() doesn't match with expected one. ", new Float(1), new Float(cs.getFloat("IntKey"))); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(" Actual value of getLong() doesn't match with expected one. ", 1, cs.getLong("IntKey")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(" Actual value of getDouble() doesn't match with expected one. ", new Double(1), new Double(cs.getDouble("IntKey"))); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(" Actual value of getByte() doesn't match with expected one. ", (byte)1, cs.getByte("IntKey")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** test wasNull() for ResultSet, this result actually is not a cursor result, but AllResults here. */
    @Test public void testWasNull() throws SQLException {
        ResultSet cs = helpExecuteQuery();
        cs.next();
        assertNotNull(cs.getObject("IntKey")); //$NON-NLS-1$
        assertFalse(cs.wasNull());
    }

    @Test public void testForwardOnly() throws Exception {
        ResultSetImpl cs = helpExecuteQuery(400, 1300, ResultSet.TYPE_FORWARD_ONLY);
        int i = 0;
        while (cs.next()) {
            i++;
            if (i <= 1200) {
                assertNotNull(cs.getPrefetch());
            } else {
                assertNull(cs.getPrefetch());
            }
            cs.getObject(1);
        }

        assertTrue(cs.isAfterLast());
        cs.close();
    }

    @Test public void testForwardOnlyPrefetchSmallFetchSize() throws Exception {
        StatementImpl statement = createMockStatement(ResultSet.TYPE_FORWARD_ONLY);
        ResultSetImpl cs = TestAllResultsImpl.helpTestBatching(statement, 10, 128, 256, true);
        for (int i = 0; i < 256; i++) {
            cs.next();
            cs.getObject(1);
        }
        Mockito.verify(statement.getDQP(), Mockito.atMost(1)).processCursorRequest(TestAllResultsImpl.REQUEST_ID, 11, 10);
        assertFalse(cs.next());
        assertTrue(cs.isAfterLast());
        cs.close();
    }

    @Test public void testOutputParameter() throws Exception {
        StatementImpl statement = createMockStatement(ResultSet.TYPE_FORWARD_ONLY);
        ResultsMessage resultsMsg = new ResultsMessage();
        resultsMsg.setResults(new List<?>[] {Arrays.asList(1, null, null), Arrays.asList(null, 2, 3)});
        resultsMsg.setLastRow(2);
        resultsMsg.setFirstRow(1);
        resultsMsg.setFinalRow(2);
        resultsMsg.setColumnNames(new String[] {"x", "out1", "out2"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        resultsMsg.setDataTypes(new String[] {"integer", "integer", "integer"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetImpl cs = new ResultSetImpl(resultsMsg, statement, null, 2);

        int count = 0;
        while (cs.next()) {
            cs.getObject(1);
            count++;
        }
        assertEquals(1, count);
        assertTrue(cs.isAfterLast());
        assertEquals(2, cs.getOutputParamValue(2));
        assertEquals(3, cs.getOutputParamValue(3));
    }

    @Test public void testXML() throws Exception {
        StatementImpl statement = createMockStatement(ResultSet.TYPE_FORWARD_ONLY);
        ResultsFuture<LobChunk> future = new ResultsFuture<LobChunk>();
        future.getResultsReceiver().receiveResults(new LobChunk("<a/>".getBytes(Charset.forName("UTF-8")), true));
        XMLType result = new XMLType();
        Mockito.stub(statement.getDQP().requestNextLobChunk(0, 0, result.getReferenceStreamId())).toReturn(future);
        ResultsMessage resultsMsg = new ResultsMessage();
        result.setEncoding("UTF-8");
        resultsMsg.setResults(new List<?>[] {Arrays.asList(result)});
        resultsMsg.setLastRow(1);
        resultsMsg.setFirstRow(1);
        resultsMsg.setFinalRow(1);
        resultsMsg.setColumnNames(new String[] {"x"}); //$NON-NLS-1$
        resultsMsg.setDataTypes(new String[] {"xml"}); //$NON-NLS-1$
        ResultSetImpl cs = new ResultSetImpl(resultsMsg, statement);
        cs.next();
        assertEquals("<a/>", cs.getString(1));
    }

    /////////////////////// Helper Method ///////////////////

    private ResultSetImpl helpExecuteQuery() {
        try {
            return helpExecuteQuery(BATCH_SIZE, 1000, ResultSet.TYPE_SCROLL_INSENSITIVE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ResultSetImpl helpExecuteQuery(int fetchSize, int totalResults, int cursorType) throws SQLException, TeiidProcessingException {
        StatementImpl statement = createMockStatement(cursorType, withSettings().stubOnly());
        return TestAllResultsImpl.helpTestBatching(statement, fetchSize, Math.min(fetchSize, totalResults), totalResults);
    }

    static StatementImpl createMockStatement(int cursorType) throws SQLException {
        return createMockStatement(cursorType, withSettings());
    }

    static StatementImpl createMockStatement(int cursorType, MockSettings mockSetting) throws SQLException {
        StatementImpl statement = mock(StatementImpl.class, mockSetting);
        DQP dqp = mock(DQP.class);
        stub(statement.getDQP()).toReturn(dqp);
        stub(statement.getResultSetType()).toReturn(cursorType);
        TimeZone tz = TimeZone.getTimeZone("GMT-06:00"); //$NON-NLS-1$
        TimeZone serverTz = TimeZone.getTimeZone("GMT-05:00"); //$NON-NLS-1$
        stub(statement.getDefaultCalendar()).toReturn(Calendar.getInstance(tz));
        stub(statement.getServerTimeZone()).toReturn(serverTz);
        return statement;
    }

    ////////////////////////Expected Results////////////////
    /** column name */
    private List<String> getBQTRSMetaData1a() {
        List<String> results = new ArrayList<String>();
        results.add("IntKey"); //$NON-NLS-1$
        return results;
    }

    /** column type */
    private List<Integer> getBQTRSMetaData1b() {
        List<Integer> results = new ArrayList<Integer>();
        results.add(Types.INTEGER);
        return results;
    }

    /** column type name*/
    private List<String> getBQTRSMetaData1c() {
        List<String> results = new ArrayList<String>();
        results.add("integer"); //$NON-NLS-1$
        return results;
    }
}
