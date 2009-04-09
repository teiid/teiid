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

package com.metamatrix.common.util.crypto.keymanage;

import com.metamatrix.core.util.UnitTestUtil;

import junit.framework.TestCase;

/** 
 * Tests FilePasswordConverter
 * @since 4.3
 */
public class TestFilePasswordConverter extends TestCase {

    private final static String INPUT_PROPERTIES_FILE = UnitTestUtil.getTestDataPath() + "/keymanage/convert.properties"; //$NON-NLS-1$
    private final static String INPUT_XML_FILE = UnitTestUtil.getTestDataPath() + "/keymanage/config.xml"; //$NON-NLS-1$
    private final static String INPUT_VDB_FILE = UnitTestUtil.getTestDataPath() + "/keymanage/ODBCvdb.VDB"; //$NON-NLS-1$
    
    private final static String KEYSTORE_FILE1 = UnitTestUtil.getTestDataPath() + "/keymanage/teiid.keystore"; //$NON-NLS-1$
    private final static String KEYSTORE_FILE2 = UnitTestUtil.getTestDataPath() + "/keymanage/other.keystore"; //$NON-NLS-1$
    
    private final static String TEMP_PROPERTIES = UnitTestUtil.getTestScratchPath() + "temp.properties"; //$NON-NLS-1$
    private final static String TEMP2_PROPERTIES = UnitTestUtil.getTestScratchPath() + "temp2.properties"; //$NON-NLS-1$
    private final static String TEMP_XML = UnitTestUtil.getTestScratchPath() + "temp.xml"; //$NON-NLS-1$
    private final static String TEMP2_XML = UnitTestUtil.getTestScratchPath() + "temp2.xml"; //$NON-NLS-1$
    private final static String TEMP_VDB = UnitTestUtil.getTestScratchPath() + "temp.vdb"; //$NON-NLS-1$
    private final static String TEMP2_VDB = UnitTestUtil.getTestScratchPath() + "temp2.vdb"; //$NON-NLS-1$
        
    public void testConvertProperties() throws Exception {
        //convert from keystore1 to keystore2
        String inputFile = INPUT_PROPERTIES_FILE;
        String outputFile = TEMP_PROPERTIES; 
        String oldKeystoreFile = KEYSTORE_FILE1;
        String newKeystoreFile = KEYSTORE_FILE2;
        
        FilePasswordConverter converter = new FilePasswordConverter(inputFile, outputFile, 
                                                                    FilePasswordConverter.FILE_TYPE_PROPERTIES, 
                                                                    oldKeystoreFile, 
                                                                    newKeystoreFile);        
        converter.convert();
        
        //check that properties were converted
        assertEquals(1, converter.converted.size());
        assertTrue(converter.converted.contains("metamatrix.common.pooling.jdbc.Password")); //$NON-NLS-1$
        assertEquals(0, converter.failedDecrypt.size());
        assertEquals(0, converter.failedEncrypt.size());
        
        
        
        
        //convert back from keystore2 to keystore1
        inputFile = TEMP_PROPERTIES;  
        outputFile = TEMP2_PROPERTIES; 
        oldKeystoreFile = KEYSTORE_FILE2;
        newKeystoreFile = KEYSTORE_FILE1;
        
        converter = new FilePasswordConverter(inputFile, outputFile, 
                                              FilePasswordConverter.FILE_TYPE_PROPERTIES, oldKeystoreFile,
                                              newKeystoreFile);   
        
        //check that properties were converted
        converter.convert();        
        assertEquals(1, converter.converted.size());
        assertTrue(converter.converted.contains("metamatrix.common.pooling.jdbc.Password")); //$NON-NLS-1$
        assertEquals(0, converter.failedDecrypt.size());
        assertEquals(0, converter.failedEncrypt.size());
        
    }
    
    
    
    public void testConvertXML() throws Exception {
        //convert from keystore1 to keystore2
        String inputFile = INPUT_XML_FILE;
        String outputFile = TEMP_XML; 
        String oldKeystoreFile = KEYSTORE_FILE1;
        String newKeystoreFile = KEYSTORE_FILE2;
        
        FilePasswordConverter converter = new FilePasswordConverter(inputFile, outputFile, 
                                                                    FilePasswordConverter.FILE_TYPE_XML, 
                                                                    oldKeystoreFile, 
                                                                    newKeystoreFile);        
        converter.convert();        
        
        //check that properties were converted
        assertEquals(4, converter.converted.size());
        assertTrue(converter.converted.contains("Password")); //$NON-NLS-1$
        assertEquals(0, converter.failedDecrypt.size());
        assertEquals(0, converter.failedEncrypt.size());
        
        
        
        
        //convert back from keystore2 to keystore1
        inputFile = TEMP_XML;  
        outputFile = TEMP2_XML; 
        oldKeystoreFile = KEYSTORE_FILE2;
        newKeystoreFile = KEYSTORE_FILE1;
        
        converter = new FilePasswordConverter(inputFile, outputFile, 
                                              FilePasswordConverter.FILE_TYPE_XML, oldKeystoreFile,
                                              newKeystoreFile);   
        
        //check that properties were converted
        converter.convert();        
        assertEquals(4, converter.converted.size());
        assertTrue(converter.converted.contains("Password")); //$NON-NLS-1$
        assertEquals(0, converter.failedDecrypt.size());
        assertEquals(0, converter.failedEncrypt.size());
        
    }
    
    
    
    public void testConvertVDB() throws Exception {
        //convert from keystore1 to keystore2
        String inputFile = INPUT_VDB_FILE;
        String outputFile = TEMP_VDB; 
        String oldKeystoreFile = KEYSTORE_FILE1;
        String newKeystoreFile = KEYSTORE_FILE2;
        
        FilePasswordConverter converter = new FilePasswordConverter(inputFile, outputFile, 
                                                                    FilePasswordConverter.FILE_TYPE_VDB, 
                                                                    oldKeystoreFile,
                                                                    newKeystoreFile);        
        converter.convert();        
        
        //check that properties were converted
        assertEquals(4, converter.converted.size());
        assertTrue(converter.converted.contains("Password")); //$NON-NLS-1$
        assertEquals(0, converter.failedDecrypt.size());
        assertEquals(0, converter.failedEncrypt.size());
        
        
        
        
        //convert back from keystore2 to keystore1
        inputFile = TEMP_VDB;  
        outputFile = TEMP2_VDB; 
        oldKeystoreFile = KEYSTORE_FILE2;
        newKeystoreFile = KEYSTORE_FILE1;
        
        converter = new FilePasswordConverter(inputFile, outputFile, 
                                              FilePasswordConverter.FILE_TYPE_VDB, oldKeystoreFile,
                                              newKeystoreFile);   
        
        //check that properties were converted
        converter.convert();        
        assertEquals(4, converter.converted.size());
        assertTrue(converter.converted.contains("Password")); //$NON-NLS-1$
        assertEquals(0, converter.failedDecrypt.size());
        assertEquals(0, converter.failedEncrypt.size());
        
    }
}
