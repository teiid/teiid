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

import org.teiid.connector.api.ConnectorException;

import junit.framework.TestCase;



public class TestRowHeaderTextSynchExecution extends TestCase {

    public TestRowHeaderTextSynchExecution(String name) {
        super(name);
    }

    /**
     * Standard test with first line being the  
     * the header row.
     * @throws Exception
     */
    public void testRowHeader() throws Exception {
        String sql = "SELECT Part_ID, Part_Name, Part_Color, Part_Weight from PARTS.PARTS";  //$NON-NLS-1$
        Util.helpTestExecution("TextParts/TextParts.vdb", "TextParts/PartsDescriptor.txt", sql, 15000, 21); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Test with header row and no Name In Source defined. 
     * Should use short name of element.  
     * @throws Exception
     */
    public void testRowHeader2() throws Exception {
        String sql = "SELECT Part_ID, Part_Name, Part_Color, Part_Weight from PARTS.PARTS_NoNameInSource";  //$NON-NLS-1$
        Util.helpTestExecution("TextParts/TextParts.vdb", "TextParts/PartsDescriptor.txt", sql, 15000, 21); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /** 
     * Test with header row and Name In Source set to that of 
     * the element's name.
     * @throws Exception
     */
    public void testRowHeader3() throws Exception {
        String sql = "SELECT Part_ID, Part_Name, Part_Color, Part_Weight from PARTS.PARTS_AlphaNameInSource";  //$NON-NLS-1$
        Util.helpTestExecution("TextParts/TextParts.vdb", "TextParts/PartsDescriptor.txt", sql, 15000, 21); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Test with header row and Name In Source set to something
     * different than the element name but matches what is used 
     * in the CSV file.
     * @throws Exception
     */
    public void testRowHeader4() throws Exception {
        String sql = "SELECT Part_ID, Part_Name, Part_Color, Part_Weight from PARTS.PARTS_AlphaDiffNameInSource";  //$NON-NLS-1$
        Util.helpTestExecution("TextParts/TextParts.vdb", "TextParts/PartsDescriptor.txt", sql, 15000, 21); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Test with multiple blank lines and a header row and 
     * no Name In Source.
     * @throws Exception
     */
    public void testRowHeader5() throws Exception {
        String sql = "SELECT Part_ID, Part_Name, Part_Color, Part_Weight from PARTS.PARTS_NoNameInSource";  //$NON-NLS-1$
        Util.helpTestExecution("TextParts/TextParts.vdb", "TextParts/PartsDescriptor_HeaderRowTest.txt", sql, 15000, 21); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Test with multiple blank lines and a header row on a   
     * specified row with name in source set.
     * @throws Exception
     */
    public void testRowHeader6() throws Exception {
        String sql = "SELECT Part_ID, Part_Name, Part_Color, Part_Weight from PARTS.PARTS_AlphaDiffNameInSource";  //$NON-NLS-1$
        Util.helpTestExecution("TextParts/TextParts.vdb", "TextParts/PartsDescriptor_HeaderRowTest.txt", sql, 15000, 21); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test with quoted header row and no Name In Source defined. 
     * Should use short name of element.  
     * @throws Exception
     */
    public void testRowHeader7() throws Exception {
        String sql = "SELECT Part_ID, Part_Name, Part_Color, Part_Weight from PARTS.PARTS_NoNameInSource";  //$NON-NLS-1$
        Util.helpTestExecution("TextParts/TextParts.vdb", "TextParts/PartsDescriptor3.txt", sql, 15000, 21); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Test with header row that contains columns with names which
     * contain spaces and have a Name In Source defined with spaces. 
     * @throws Exception
     */
    public void testRowHeader8() throws Exception {
        String sql = "SELECT Part_ID, Part_Name, Part_Color, Part_Weight from PARTS.PARTS_SpaceNameInSource";  //$NON-NLS-1$
        Util.helpTestExecution("TextParts/TextParts.vdb", "TextParts/PartsDescriptor4.txt", sql, 15000, 21); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Test with bad header row.  The model uses column names for  
     * name in source.  This should result in an error stating the 
     * column was not found. 
     * @throws Exception
     */
    public void testRowHeader_Error() throws Exception {
        String sql = "SELECT Part_ID, Part_Name, Part_Color, Part_Weight from PARTS.PARTS_AlphaNameInSource";  //$NON-NLS-1$
        try {
            Util.helpTestExecution("TextParts/TextParts.vdb", "TextParts/PartsDescriptor4.txt", sql, 15000, 21); //$NON-NLS-1$ //$NON-NLS-2$
            fail("Should have received ConnectorException due to an invalid header row being defined."); //$NON-NLS-1$
        } catch (ConnectorException ce ) {
        	assertEquals("'SELECT PARTS_AlphaNameInSource.Part_Id, PARTS_AlphaNameInSource.Part_Name, PARTS_AlphaNameInSource.Part_Color, PARTS_AlphaNameInSource.Part_Weight FROM PARTS_AlphaNameInSource' cannot be translated by the TextTranslator. Column Part_Id not found for element Parts.PARTS_AlphaNameInSource.Part_Id.  Verify column name \"Part_Id\" is defined in the header row of the text file and that the header row number is correctly defined in the descriptor file.", ce.getMessage()); //$NON-NLS-1$
        }
    }
    
    /**
     * Test with bad header row number.  The model uses column names 
     * for name in source.  The descriptor defines the header row 
     * outside the skipped header lines.  In this case, the connector 
     * should default to the last line of the skipped header lines and 
     * log an error. 
     * @throws Exception
     */
    public void testRowHeader_Error2() throws Exception {
        String sql = "SELECT Part_ID, Part_Name, Part_Color, Part_Weight from PARTS.PARTS_NoNameInSource";  //$NON-NLS-1$
        Util.helpTestExecution("TextParts/TextParts.vdb", "TextParts/PartsDescriptor4.txt", sql, 15000, 21); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
}
