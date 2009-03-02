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

package com.metamatrix.connector.text;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.ConnectorHost;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.query.unittest.TimestampUtil;

/**
 * TODO: test cancel
 */
public class TestTextSynchExecution extends TestCase {
	private static final String DEFAULT_DESC_FILE = UnitTestUtil.getTestDataPath() + "/testDescriptorDelimited.txt"; //$NON-NLS-1$
	
    public TestTextSynchExecution(String name) {
        super(name);
    }

    public void testNextBatch2() throws Exception {
        String sql = "SELECT ID, PDate, Author FROM Library"; //$NON-NLS-1$
        ConnectorHost host = Util.getConnectorHostWithFakeMetadata(DEFAULT_DESC_FILE);
        int expectedRows = 2;
        List results = host.executeCommand(sql);
        assertEquals("Get batch size doesn't match expected one. ", expectedRows, results.size()); //$NON-NLS-1$

        // expected values
        List[] expected = new List[2];
        List value1 = new ArrayList();
        value1.add(String.valueOf(1));
        value1.add(TimestampUtil.createDate(103, 2, 25));
        value1.add("Blind"); //$NON-NLS-1$

        List value2 = new ArrayList();
        value2.add(String.valueOf(2));
        value2.add(TimestampUtil.createDate(98, 3, 29));
        value2.add("Antipop"); //$NON-NLS-1$
        expected[0] = value1;
        expected[1] = value2;

        for(int i = 0; i < 2; i++) {
            assertEquals(" Actual value doesn't match with expected one.", expected[i], results.get(i)); //$NON-NLS-1$
        }
    }

    /**
     * test defect 13066
     *
     * Test multibatch execution.
     */
    public void testDefect13066() throws Exception {
        String sql = "SELECT TRADEID FROM SummitData.SUMMITDATA"; //$NON-NLS-1$
        Util.helpTestExecution("summitData/TextFileTest_1.vdb", "SummitData_Descriptor.txt", sql, 500, 4139); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * test defect 13366
     *
     * Test zero size first batch
     */
    public void testDefect13368() throws Exception {
        String sql = "SELECT RATE, DESK FROM SummitData.SUMMITDATA"; //$NON-NLS-1$
        Util.helpTestExecution("summitData/TextFileTest_1.vdb", "SummitData_Descriptor.txt", sql, 5, 4139); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * test defect 13371
     *
     * test fixed length field file while querying a subset of the columns
     */
    public void testDefect13371() throws Exception {
        String sql = "  SELECT SUMMITEXTRACTCDM.START, SUMMITEXTRACTCDM.SUMMIT_ID, SUMMITEXTRACTCDM.CURRENCY, SUMMITEXTRACTCDM.AMOUNT, SUMMITEXTRACTCDM.MATURITY, SUMMITEXTRACTCDM.RATE, SUMMITEXTRACTCDM.DESK, SUMMITEXTRACTCDM.CDM_ID FROM SUMMITEXTRACTCDM"; //$NON-NLS-1$
        Util.helpTestExecution("summitData/TextFiles.vdb", "SummitExtractCDM_Descriptor.txt", sql, 500, 52); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDefect11402() throws Exception {
        String sql = "SELECT Part_ID, Part_Name, Part_Color, Part_Weight from PARTS.PARTS";  //$NON-NLS-1$
        Util.helpTestExecution("TextParts/TextParts.vdb", "/TextParts/PartsDescriptor.txt", sql, 15000, 21); //$NON-NLS-1$ //$NON-NLS-2$
    }
        
    /** test case 4151 */
    public void testCase4151() throws Exception {
        String sql = "SELECT COLA, COLB, COLC FROM ThreeColString_Text.testfile"; //$NON-NLS-1$
        Util.helpTestExecution("case4151/MM_00004151.vdb", "testfile-descriptor.txt", sql, 15000, 5); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
}
