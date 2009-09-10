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

package com.metamatrix.cdk.api;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.core.commandshell.ConnectorResultUtility;
import com.metamatrix.core.util.StringUtil;

public class TestConnectorResultUtility extends TestCase {
    List bob1;
    List bob2;
    List jim1;
    List bobShort;
    List bobMultiLineWithTabs;
    
    List nullList;
    
    List results1;
    List results2;
    
    public TestConnectorResultUtility(String name) {
        super(name);
    }
    
    public void testSame() {  
        results1.add(bob1);
        results1.add(jim1);
        
        results2.add(bob1);
        results2.add(jim1);

        String expected = ConnectorResultUtility.resultsToString(results1);
        String actual = ConnectorResultUtility.resultsToString(results2);        
        String result = ConnectorResultUtility.compareResultsStrings(expected, actual);
        assertNull(result);
    }
    
    public void testDifferent() {
        results1.add(bob1);
        results1.add(jim1);

        results2.add(bob2);
        results2.add(jim1);

        String expected = ConnectorResultUtility.resultsToString(results1);
        String actual = ConnectorResultUtility.resultsToString(results2);
        String result = ConnectorResultUtility.compareResultsStrings(expected, actual);
        assertEquals("CompareResults Error: Value mismatch at row 1 and column 1: expected = 45, actual = 25", result); //$NON-NLS-1$
    }
    
    public void testTooFewRows() {  
        results1.add(bob1);
        results1.add(jim1);

        results2.add(bob1);

        String expected = ConnectorResultUtility.resultsToString(results1);
        String actual = ConnectorResultUtility.resultsToString(results2);        
        String result = ConnectorResultUtility.compareResultsStrings(expected, actual);
        assertEquals("CompareResults Error: Expected 3 records but received only 2", result); //$NON-NLS-1$
    }

    public void testTooManyRows() {  
        results1.add(bob1);

        results2.add(bob1);
        results2.add(jim1);

        String expected = ConnectorResultUtility.resultsToString(results1);
        String actual = ConnectorResultUtility.resultsToString(results2);        
        String result = ConnectorResultUtility.compareResultsStrings(expected, actual);
        assertEquals("CompareResults Error: Expected 2 records but received 3", result); //$NON-NLS-1$
    }

    public void testNullColumns() {  
        results1.add(bob1);

        results2.add(nullList);

        String expected = ConnectorResultUtility.resultsToString(results1);
        String actual = ConnectorResultUtility.resultsToString(results2);        
        String result = ConnectorResultUtility.compareResultsStrings(expected, actual);
        assertEquals("CompareResults Error: Value mismatch at row 1 and column 0: expected = bob, actual = <null>", result); //$NON-NLS-1$
    }

    public void testNullColumnInFirstRowWithDataLater() {
        results1.add(nullList);
        results1.add(bob1);
        String result = ConnectorResultUtility.resultsToString(results1);
        assertEquals(
            "col1\tcol2" + StringUtil.LINE_SEPARATOR +   //$NON-NLS-1$
            "<null>\t<null>" + StringUtil.LINE_SEPARATOR +    //$NON-NLS-1$
            "bob\t45" + StringUtil.LINE_SEPARATOR,            //$NON-NLS-1$
            result); 
    }

    public void testNullColumnInFirstRowWithNoDataLater() {
        results1.add(nullList);
        results1.add(nullList);
        String result = ConnectorResultUtility.resultsToString(results1);
        assertEquals(
            "col1\tcol2" + StringUtil.LINE_SEPARATOR +   //$NON-NLS-1$
            "<null>\t<null>" + StringUtil.LINE_SEPARATOR +   //$NON-NLS-1$
            "<null>\t<null>" + StringUtil.LINE_SEPARATOR,    //$NON-NLS-1$
            result); 
    }

    public void testTooFewColumns() {  
        results1.add(bob1);

        results2.add(bobShort);

        String expected = ConnectorResultUtility.resultsToString(results1);
        String actual = ConnectorResultUtility.resultsToString(results2);        
        String result = ConnectorResultUtility.compareResultsStrings(expected, actual);
        assertEquals("Incorrect number of columns at row = 0, expected = 2, actual = 1", result); //$NON-NLS-1$
    }

    public void testTooManyColumns() {  
        results1.add(bobShort);

        results2.add(bob1);

        String expected = ConnectorResultUtility.resultsToString(results1);
        String actual = ConnectorResultUtility.resultsToString(results2);        
        String result = ConnectorResultUtility.compareResultsStrings(expected, actual);
        assertEquals("Incorrect number of columns at row = 0, expected = 1, actual = 2", result); //$NON-NLS-1$
    }
    
    public void testTabsAndNewLines() {
        results1.add(bobMultiLineWithTabs);
        String result = ConnectorResultUtility.resultsToString(results1);
        //tabs and newlines in Strings in the result object end up as literal "\t" and "\n" values in the string version
        assertEquals("col1\tcol2" + StringUtil.LINE_SEPARATOR + "bob\\tsmith\\nEsquire\t45" + StringUtil.LINE_SEPARATOR, result); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /* 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        bob1 = new ArrayList();
        bob1.add("bob"); //$NON-NLS-1$
        bob1.add(new Integer(45));
        jim1 = new ArrayList();
        jim1.add("jim"); //$NON-NLS-1$
        jim1.add(new Integer(19));

        bob2 = new ArrayList();
        bob2.add("bob"); //$NON-NLS-1$
        bob2.add(new Integer(25));
        
        bobShort = new ArrayList();
        bobShort.add("bob"); //$NON-NLS-1$
        
        bobMultiLineWithTabs = new ArrayList();
        bobMultiLineWithTabs.add("bob\tsmith\nEsquire"); //$NON-NLS-1$
        bobMultiLineWithTabs.add(new Integer(45));
        
        nullList = new ArrayList();
        nullList.add(null);
        nullList.add(null);
               
        results1 = new ArrayList();
        results2 = new ArrayList();
    }
}
