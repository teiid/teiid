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

package com.metamatrix.dqp.spi.basic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import com.metamatrix.core.util.UnitTestUtil;

import junit.framework.TestCase;


/** 
 * Tests the DQP Tracking service implementation which uses a CommandLogger service
 * provider
 */
public class TestFileCommandLogger extends TestCase {

    String filename = UnitTestUtil.getTestScratchPath() + "/commandLog.txt"; //$NON-NLS-1$
    
    public void setUp() {
        File file = new File(filename);
        if (file.exists()) {
            file.delete();
        }
    }
    
    /**
     * Constructor for TestConnectorCapabilitiesFinder.
     * @param name
     */
    public TestFileCommandLogger(String name) {
        super(name);
    }
    
    // ========================================================================================================
    // tests
    // ========================================================================================================
    
    public void testLog() throws Exception {
        
        Properties props = new Properties();
        props.setProperty(FileCommandLogger.LOG_FILE_NAME_PROPERTY, this.filename);
        FileCommandLogger logger = new FileCommandLogger();
        logger.initialize(props);
        
        logger.transactionStart(System.currentTimeMillis(), "2112", "5150", "nuge", "myVDB", "2");  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        logger.userCommandStart(System.currentTimeMillis(), "13", "2112", "5150", "myAppName", "nuge", "myVDB", "2", "SELECT * FROM MyModel.MyTable"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
        logger.dataSourceCommandStart(System.currentTimeMillis(), "13", 51l, "18", "MyPhyModel", "MyBinding", "2112", "nuge", "SELECT * FROM MyPhyModel.MyTable", null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
        logger.dataSourceCommandEnd(System.currentTimeMillis(), "13", 51l, "18", "MyPhyModel", "MyBinding", "2112", "nuge", 777, false, false, null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ 
        logger.userCommandEnd(System.currentTimeMillis(), "13", "2112", "5150", "nuge", "myVDB", "2", 10000, false, false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ 
        logger.transactionEnd(System.currentTimeMillis(), "2112", "5150", "nuge", "myVDB", "2", true);  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        
        logger.close();
        
        File file = new File(filename);
        assertTrue(file.exists());

        BufferedReader in = null;
        int linecount=0;
        try{
            in = new BufferedReader(new FileReader(file));
            
            for ( ; in.readLine() != null; linecount++) {
                //empty
            }
        } finally {
            try{
                if (in != null){
                    in.close();
                }
            } catch (IOException e){
                //ignore
            }   
        }        
        int expectedLineCount = 6; // expected number of lines in log file
        assertEquals(expectedLineCount, linecount);
    }
    
}
