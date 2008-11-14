/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.client.impl;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.dqp.client.Results;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.dqp.message.ResultsMessage;


/** 
 * @since 4.3
 */
public class TestResultBatch extends TestCase {

    private void helpTestResults(ResultBatch batch, int start, int end) throws Exception {
        for (int i = start; i <= end; i++) {
            assertEquals(new Integer(i-start), batch.getValue(i, 1));
            assertEquals(Integer.toString(i-start), batch.getValue(i, 2));
        }
    }
    
    private ResultsMessage helpGetResultsMessage() {
        ResultsMessage message = new ResultsMessage();
        message.setColumnNames(new String[] {"intcol", "stringcol"}); //$NON-NLS-1$ //$NON-NLS-2$
        message.setFinalRow(2000);
        message.setFirstRow(501);
        message.setLastRow(1000);
        
        ParameterInfo[] params = {};
        message.setParameters(Arrays.asList(params));
        List[] rows = new List[500];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = Arrays.asList(new Object[] {new Integer(i), Integer.toString(i)});
        }
        message.setResults(rows);
        
        List warnings = Arrays.asList(new Exception[] {new Exception("warning1"), new Exception("warning2")}); //$NON-NLS-1$ //$NON-NLS-2$
        message.setWarnings(warnings);
        return message;
    }
    
    public void testConstructor_ResultsMessage() throws Exception {
        //TODO test with non-null values for connection holder and requestID
        ResultBatch batch = new ResultBatch(helpGetResultsMessage(), false, 0, null);
        
        assertFalse(batch.isUpdate());
        assertEquals(501, batch.getBeginRow());
        assertEquals(1000, batch.getEndRow());
        assertEquals(2, batch.getColumnCount());
        assertEquals(500, batch.getRowCount());
        assertEquals(false, batch.isLast());
        helpTestResults(batch, batch.getBeginRow(), batch.getEndRow());
        
        assertEquals(0, batch.getParameterCount());
        
        assertEquals(2, batch.getWarnings().length);
        assertTrue(batch.getWarnings()[0].getClass().equals(Exception.class));
        assertEquals("warning1", batch.getWarnings()[0].getMessage()); //$NON-NLS-1$
        assertTrue(batch.getWarnings()[1].getClass().equals(Exception.class));
        assertEquals("warning2", batch.getWarnings()[1].getMessage()); //$NON-NLS-1$
    }
    
    public void testGetValue_Validation() throws Exception {
        ResultBatch batch = new ResultBatch(helpGetResultsMessage(), false, 0, null);
        try {
            batch.getValue(50, 1);
            fail("Should have thrown an exception"); //$NON-NLS-1$
        } catch (MetaMatrixProcessingException e) {
            
        }
        try {
            batch.getValue(510, 0);
            fail("Should have thrown an exception"); //$NON-NLS-1$
        } catch (MetaMatrixProcessingException e) {
            
        }
    }
    
    public void testGetParameterType_Validation() throws Exception {
        ResultBatch batch = new ResultBatch(helpGetResultsMessage(), false, 0, null);
        try {
            batch.getParameterType(0);
            fail("Should have thrown an exception"); //$NON-NLS-1$
        } catch (MetaMatrixProcessingException e) {
            
        }
        try {
            batch.getParameterType(10);
            fail("Should have thrown an exception"); //$NON-NLS-1$
        } catch (MetaMatrixProcessingException e) {
            
        }
    }
    
    public void testGetUpdateCount_Validation() {
        ResultBatch batch = new ResultBatch(helpGetResultsMessage(), false, 0, null);
        try {
            batch.getUpdateCount();
            fail("Should have thrown an exception"); //$NON-NLS-1$
        } catch (MetaMatrixComponentException e) {
            
        }
    }
    
    public void testIsUpdate_Defect18449() {
        ResultBatch batch = new ResultBatch(helpGetResultsMessage(), true, 0, null);
        assertTrue(batch.isUpdate());
    }
    
    public void testStoredProcResults() throws Exception {
        ResultsMessage message = new ResultsMessage();
        message.setColumnNames(new String[] {"intcol", "stringcol", "param1", "param2"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        message.setFinalRow(500);
        message.setFirstRow(1);
        message.setLastRow(500);
        
        ParameterInfo[] params = {new ParameterInfo(ParameterInfo.IN, 0),
            new ParameterInfo(ParameterInfo.IN, 0),
            new ParameterInfo(ParameterInfo.INOUT, 0),
            new ParameterInfo(ParameterInfo.OUT, 0)};
        message.setParameters(Arrays.asList(params));
        List[] rows = new List[500];
        for (int i = 0; i < 498; i++) {
            rows[i] = Arrays.asList(new Object[] {new Integer(i), Integer.toString(i), null, null});
        }
        rows[498] = Arrays.asList(new Object[] {null, null, new Integer(1), null});
        rows[499] = Arrays.asList(new Object[] {null, null, null, new Integer(2)});
        message.setResults(rows);
        
        ResultBatch batch = new ResultBatch(message, false, 0, null);
        assertEquals(498, batch.getRowCount());
        
        assertEquals(1, batch.getBeginRow());
        assertEquals(498, batch.getEndRow());
        assertEquals(2, batch.getColumnCount());
        
        assertEquals(4, batch.getParameterCount());
        
        assertEquals(Results.PARAMETER_TYPE_IN, batch.getParameterType(1));
        assertEquals(Results.PARAMETER_TYPE_IN, batch.getParameterType(2));
        assertEquals(Results.PARAMETER_TYPE_INOUT, batch.getParameterType(3));
        assertEquals(Results.PARAMETER_TYPE_OUT, batch.getParameterType(4));
        
        assertEquals(new Integer(1), batch.getOutputParameter(3));
        assertEquals(new Integer(2), batch.getOutputParameter(4));
    }
    
    public void testStoredProcResultsWithResultSet() throws Exception {
        ResultsMessage message = new ResultsMessage();
        message.setColumnNames(new String[] {"intcol", "stringcol", "param1", "param2"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        message.setFinalRow(500);
        message.setFirstRow(1);
        message.setLastRow(500);
        
        ParameterInfo[] params = {
            new ParameterInfo(ParameterInfo.RESULT_SET, 2),
            new ParameterInfo(ParameterInfo.IN, 0),
            new ParameterInfo(ParameterInfo.IN, 0),
            new ParameterInfo(ParameterInfo.INOUT, 0),
            new ParameterInfo(ParameterInfo.OUT, 0)};
        message.setParameters(Arrays.asList(params));
        List[] rows = new List[500];
        for (int i = 0; i < 498; i++) {
            rows[i] = Arrays.asList(new Object[] {new Integer(i), Integer.toString(i), null, null});
        }
        rows[498] = Arrays.asList(new Object[] {null, null, new Integer(1), null});
        rows[499] = Arrays.asList(new Object[] {null, null, null, new Integer(2)});
        message.setResults(rows);
        
        ResultBatch batch = new ResultBatch(message, false, 0, null);
        assertEquals(498, batch.getRowCount());
        
        assertEquals(1, batch.getBeginRow());
        assertEquals(498, batch.getEndRow());
        assertEquals(2, batch.getColumnCount());
        
        assertEquals(4, batch.getParameterCount());
        
        assertEquals(Results.PARAMETER_TYPE_IN, batch.getParameterType(1));
        assertEquals(Results.PARAMETER_TYPE_IN, batch.getParameterType(2));
        assertEquals(Results.PARAMETER_TYPE_INOUT, batch.getParameterType(3));
        assertEquals(Results.PARAMETER_TYPE_OUT, batch.getParameterType(4));
        
        assertEquals(new Integer(1), batch.getOutputParameter(3));
        assertEquals(new Integer(2), batch.getOutputParameter(4));
    }
}
