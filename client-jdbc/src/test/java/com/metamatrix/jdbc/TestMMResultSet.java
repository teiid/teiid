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

import static org.mockito.Mockito.*;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.dqp.client.ClientSideDQP;

public class TestMMResultSet extends TestCase {

    public TestMMResultSet(String name) {
        super(name);        
    }
    
    /** test next() without walking through */
    public void testNext1() throws SQLException {  
        ResultSet cs =  helpExecuteQuery();
        assertEquals(" Actual doesn't match with expected. ", new Integer(0), new Integer(cs.getRow())); //$NON-NLS-1$
        cs.close();       
    } 
    
    /** test next() with walking through all the rows and compare records */
    public void testNext2() throws SQLException {  
        List[] expected = TestAllResultsImpl.exampleResults1(1000);
        MMResultSet cs =  helpExecuteQuery();

        int i=0;
        while(cs.next()) { 
           assertEquals(" Actual doesn't match with expected. ", expected[i], cs.getCurrentRecord()); //$NON-NLS-1$
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
    public void testNextBeyondEnd_fetchEqualsCount() throws Exception {
        helpTestNextBeyondResultSet(1000, 1000);
    }

    /** Test stability when next() is called beyond the rowcount. */
    public void testNextBeyondEnd_fetchLessThanCount() throws Exception {
        helpTestNextBeyondResultSet(100, 1000);
    }
    
    /** Test stability when next() is called beyond the rowcount with one more row. */
    public void testNextBeyondEnd_fetchLessThanCount1() throws Exception {
        helpTestNextBeyondResultSet(100, 101);
    }

    /** Test stability when next() is called beyond the rowcount. */
    public void testNextBeyondEnd_fetchLessThanCountNonMultiple() throws Exception {
        helpTestNextBeyondResultSet(120, 1000);
    }

    /** Test stability when next() is called beyond the rowcount. */
    public void testNextBeyondEnd_fetchGreaterThanCount() throws Exception {
        helpTestNextBeyondResultSet(300, PROC_BATCH_SIZE);
    }

    /** Test stability when next() is called beyond the rowcount. */
    public void testNextBeyondEnd_fetchGreaterThanCountNonMultiple() throws Exception {
        helpTestNextBeyondResultSet(310, PROC_BATCH_SIZE-50);
    }

    /** Test stability when next() is called beyond the rowcount. */
    public void testNextBeyondEnd_fetchGreaterThanCountNonMultiple2() throws Exception {
        helpTestNextBeyondResultSet(300, PROC_BATCH_SIZE+10);
    }
    
    /** Test that the returned results walks through all results if
     * fetchSize &lt; rows &lt; proc batch size.
     * Test for defect 11356
     */
    public void testNextBeyondEnd_fetchLessThanCount_ResultsBetweenFetchAndProcBatch() throws Exception {
        helpTestNextBeyondResultSet(30, PROC_BATCH_SIZE-25);
    }

    public void helpTestNextBeyondResultSet(int fetchSize, int numRows) throws Exception {
        ResultSet cs = helpExecuteQuery(fetchSize, numRows);
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
    public void testBidirection() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
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
    public void testHasNext1() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
        assertEquals(" hasNext() doesn't match expected value. ", true, cs.hasNext()); //$NON-NLS-1$
        cs.close();          
    }

    /** test hasNext() with blocking for the Next batch -- triggering point */
    public void testHasNext2() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
        cs.absolute(100);
        assertEquals(" hasNext() doesn't match expected value. ", true, cs.hasNext());  //$NON-NLS-1$
        cs.close();       
    }
    
    /** test hasNext() with nextBatch!=null -- short response */
    public void testHasNext3() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
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
    public void testHasNext4() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
        cs.absolute(1000);
        assertTrue(!cs.hasNext());
        cs.close();       
    }
    
    /** walk all way through from the end back to first row */
    public void testPrevious1() throws SQLException {  
        MMResultSet cs = helpExecuteQuery();
        List[] expected = TestAllResultsImpl.exampleResults1(1000);
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
    public void testPrevious2() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
        cs.absolute(290);

        // cursor is at the row of 289 now
        assertTrue(cs.previous());
        assertEquals(" current row doesn't match expected value. ", 289, cs.getRow()); //$NON-NLS-1$
        cs.close();     
    }

    /** walk all way through from the end back to first row */
    public void testPrevious3() throws SQLException {  
        //large batch size
        MMResultSet cs = helpExecuteQuery(600, 10000);
        List[] expected = TestAllResultsImpl.exampleResults1(10000);
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
    public void testPrevious4() throws SQLException {  
        //small batch size
        MMResultSet cs = helpExecuteQuery(50, 1000);
        List[] expected = TestAllResultsImpl.exampleResults1(1000);
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
    public void testChangeDirection() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
        cs.absolute(291);
        cs.previous();
        
        assertEquals(" current row doesn't match expected value. ", 290, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }
             
    public void testIsFirst() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
        cs.next();
        assertTrue(cs.isFirst());
        cs.close();
    }

    /** test cursor is in the middle of all batches */
    public void testIsLast1() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 
        cs.next();
        assertTrue(!cs.isLast());
        cs.close();     
    }

    /** test cursor at the triggering point -- blocking case*/
    public void testIsLast2() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
        
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
    public void testIsLast3() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
        cs.absolute(1000);
        assertTrue(cs.isLast());
        cs.close();
    }        
 
    public void testIsBeforeFirst() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 
        assertTrue(cs.isBeforeFirst());
        cs.close();        
    }

    public void testBeforeFirst() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 
        
        // move to row 1
        cs.next();
        assertEquals(" Current row number doesn't match with expected one.", 1, cs.getRow()); //$NON-NLS-1$
        
        // move back to before first row
        cs.beforeFirst();
        assertTrue(cs.isBeforeFirst());
        cs.close();           
    }

    public void testFirst() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 
        
        // move to row #2
        cs.next();
        cs.next();
        assertEquals(" Current row number doesn't match with expected one.", 2, cs.getRow()); //$NON-NLS-1$
        
        // move back to the 1st row
        cs.first();
        assertEquals(" Current row number doesn't match with expected one.", 1, cs.getRow()); //$NON-NLS-1$
        cs.close();           
    }

    public void testAfterLast() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 
        cs.afterLast();
        assertTrue(cs.isAfterLast());   
        cs.close();
    }
    
    /** right after the last row */
    public void testIsAfterLast1() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 
        cs.absolute(1000);
        cs.next();
        assertTrue(cs.isAfterLast());
        cs.close();
    }

    /** right before the first */
    public void testIsAfterLast2() throws Exception {
        MMResultSet cs =  helpExecuteQuery(); 
        assertTrue(!cs.isAfterLast());
        cs.close();
    }
      
    /** absolute with cursor movement backward in the same batch -- absolute(positive) */
    public void testAbsolute1() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 
        
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
    public void testAbsolute2() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 
        
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
    public void testAbsolute3() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 
        
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
    public void testAbsolute4() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 
        
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
    public void testAbsolute5() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 
        
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
    public void testAbsolute6() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 
        cs.absolute(1005);
        // Cursor should be after last row. getRow() should return 0 because
        // cursor is not on a valid row
        assertEquals(" Current row number doesn't match with expected one.", 0, cs.getRow());           //$NON-NLS-1$
        cs.close();
    }
               
    /** relative(positive) -- forward to another batch */           
    public void testRelative1() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 

        // move to the row #3
        cs.absolute(3);
        assertEquals(" Current row number doesn't match with expected one.", 3, cs.getRow()); //$NON-NLS-1$
        
        // move to the row #140
        cs.relative(137);
        assertEquals(" Current row number doesn't match with expected one.", 140, cs.getRow()); //$NON-NLS-1$
        cs.close();       
    }

    /** relative(negative) -- backward to another batch */          
    public void testRelative2() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 

        // move to the row #137
        cs.absolute(137);
        assertEquals(" Current row number doesn't match with expected one.", 137, cs.getRow()); //$NON-NLS-1$
        
        // move to the row #4
        cs.relative(-133);
        assertEquals(" Current row number doesn't match with expected one.", 4, cs.getRow()); //$NON-NLS-1$
        cs.close();      
    }  

    /** relative(negative) -- backward to triggering point or blocking batch */          
    public void testRelative3() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 

        // move to the row #137
        cs.absolute(137);
        assertEquals(" Current row number doesn't match with expected one.", 137, cs.getRow()); //$NON-NLS-1$
        
        // move to the row #100
        cs.relative(-37);
        assertEquals(" Current row number doesn't match with expected one.", 100, cs.getRow()); //$NON-NLS-1$
        cs.close();      
    }  
                           
    /** relative(negative) -- backward to triggering point or blocking batch */          
    public void testRelative4() throws SQLException {
        MMResultSet cs =  helpExecuteQuery(); 

        // move to the row #237 in the third batch, so that the fourth batch has been requested when we switch direction
        cs.absolute(237);
        assertEquals(" Current row number doesn't match with expected one.", 237, cs.getRow()); //$NON-NLS-1$
        
        // move to the row #37
        cs.relative(-200);
        assertEquals(" Current row number doesn't match with expected one.", 37, cs.getRow()); //$NON-NLS-1$
        cs.close();      
    }  
                           
    /** in the first fetched batch */                             
    public void testGetRow1() throws SQLException {
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
    public void testGetRow2() throws SQLException {
        ResultSet cs =  helpExecuteQuery();

        cs.next();        
        assertEquals(" Current row number doesn't match with expected one.", 1, cs.getRow()); //$NON-NLS-1$
        cs.close();
    }

    /** in the triggering point -- blocking  */                             
    public void testGetRow3() throws SQLException {
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
            
    public void testGetCurrentRecord() throws SQLException {
        ResultSet cs =  helpExecuteQuery();
        cs.absolute(103);
        assertEquals(" Current record doesn't match with expected one.", new Integer(103), ((MMResultSet)cs).getCurrentRecord().get(0));                 //$NON-NLS-1$
        cs.close();
    }

    /** test close() without walking through any of the record*/
    public void testClose() throws SQLException {  
        MMResultSet cs =  helpExecuteQuery();
        assertEquals(" Actual doesn't match with expected. ", new Integer(0), new Integer(cs.getRow())); //$NON-NLS-1$
        cs.close();          
    } 
    
    /** test basic results-related metadata */
    public void testGetMetaData() throws SQLException {    
        MMResultSet cs =  helpExecuteQuery();
        
        // check result set metadata
        // expected column info.
        List columnName = getBQTRSMetaData1a();
        List columnType = getBQTRSMetaData1b();
        List columnTypeName = getBQTRSMetaData1c();

        ResultSetMetaData rm = cs.getMetaData();
        assertNotNull(rm);
            
        for (int j = 1; j <= rm.getColumnCount(); j++) {  
            assertEquals(" Actual doesn't match with expected. ", columnName.get(j-1), rm.getColumnName(j)); //$NON-NLS-1$
            assertEquals(" Actual doesn't match with expected. ", columnType.get(j-1), new Integer(rm.getColumnType(j))); //$NON-NLS-1$
            assertEquals(" Actual doesn't match with expected. ", columnTypeName.get(j-1), rm.getColumnTypeName(j)); //$NON-NLS-1$
        }
        
        cs.close();     
    }

    public void testFindColumn() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
        ResultSetMetaData rm = cs.getMetaData();
        assertNotNull(rm);
        //assertEquals(" Actual columnName doesn't match with expected. ", 1, cs.findColumn("BQT1.MediumA.IntKey"));            
        assertEquals(" Actual columnName doesn't match with expected. ", 1, cs.findColumn("IntKey"));                       //$NON-NLS-1$ //$NON-NLS-2$
        cs.close();             
    }
    
    public void testFindNonExistentColumn() throws SQLException {
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
    
    public void testGetStatement() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();     
        assertNotNull(cs.getStatement());
        cs.close();
    }
        
    public void testGetPlanDescription() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
        assertNotNull(cs);
        
        assertNull((cs.getStatement()).getPlanDescription());
        cs.close();
    }
    
    /** getObject(String) */ 
    public void testGetObject2() throws SQLException {
        ResultSet cs =  helpExecuteQuery();
        
        // move to the 1st row
        cs.next();  
        assertEquals(" Actual value doesn't match with expected. ", new Integer(1), cs.getObject("IntKey")); //$NON-NLS-1$ //$NON-NLS-2$
        cs.close();   
    }

    public void testGetWarnings() throws SQLException {
        ResultSet cs =  helpExecuteQuery();
        assertNull(cs.getWarnings());
        cs.close();
    }
    
    public void testGetCursorName() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
        assertNull(cs.getCursorName()); 
        cs.close();
    }
    
    public void testAllGetters() throws SQLException {
        MMResultSet cs =  helpExecuteQuery();
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
    public void testWasNull() throws SQLException {
        ResultSet cs = helpExecuteQuery();
        cs.next();
        assertNotNull(cs.getObject("IntKey")); //$NON-NLS-1$
        assertFalse(cs.wasNull());
    }
 
    /** test getProcessingTime() -- include test for getProcessingTimestamp() and getCompletedTimestamp() */
    public void testGetProcessingTime() throws SQLException {  
        MMResultSet cs =  helpExecuteQuery();
        assertTrue(cs.getProcessingTime() == cs.getCompletedTimestamp().getTime() - 1);
        cs.close();       
    } 
         
    /////////////////////// Helper Method ///////////////////

    private MMResultSet helpExecuteQuery() throws SQLException {
        return helpExecuteQuery(400, 1000);
    }
    
    private MMResultSet helpExecuteQuery(int fetchSize, int totalResults) throws SQLException {
        MMStatement statement = createMockStatement();
        try {
			return TestAllResultsImpl.helpTestBatching(statement, fetchSize, Math.min(fetchSize, totalResults), totalResults);
		} catch (MetaMatrixProcessingException e) {
			throw new SQLException(e.getMessage());
		} catch (InterruptedException e) {
			throw new SQLException(e.getMessage());
		} catch (ExecutionException e) {
			throw new SQLException(e.getMessage());
		}
    }

	static MMStatement createMockStatement() throws SQLException {
		MMStatement statement = mock(MMStatement.class);
		stub(statement.getDQP()).toReturn(mock(ClientSideDQP.class));
		stub(statement.getResultSetType()).toReturn(
				ResultSet.TYPE_SCROLL_INSENSITIVE);
		TimeZone tz = TimeZone.getTimeZone("GMT-06:00"); //$NON-NLS-1$
		TimeZone serverTz = TimeZone.getTimeZone("GMT-05:00"); //$NON-NLS-1$
		stub(statement.getDefaultCalendar()).toReturn(Calendar.getInstance(tz));
		stub(statement.getServerTimeZone()).toReturn(serverTz);
		return statement;
	}

    ////////////////////////Expected Results////////////////
    /** column name */
    private List getBQTRSMetaData1a() {
        List results = new ArrayList();
        results.add("IntKey"); //$NON-NLS-1$
        return results;   
    }

    /** column type */
    private List getBQTRSMetaData1b() {
        List results = new ArrayList();
        results.add(new Integer(Types.INTEGER));
        return results;   
    }

    /** column type name*/
    private List getBQTRSMetaData1c() {
        List results = new ArrayList();
        results.add("integer"); //$NON-NLS-1$
        return results;   
    }               
}
