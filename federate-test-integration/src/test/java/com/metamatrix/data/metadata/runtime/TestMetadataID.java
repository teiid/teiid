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

package com.metamatrix.data.metadata.runtime;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.connector.language.IElement;
import com.metamatrix.connector.language.IGroup;
import com.metamatrix.connector.language.IProcedure;
import com.metamatrix.connector.language.IQuery;
import com.metamatrix.connector.language.ISelectSymbol;
import com.metamatrix.connector.metadata.runtime.MetadataID;
import com.metamatrix.core.util.UnitTestUtil;

/**
 */
public class TestMetadataID extends TestCase {

    private static TranslationUtility CONNECTOR_METADATA_UTILITY = createTranslationUtility(getTestVDBName());

    /**
     * Constructor for TestMetadataID.
     * @param name
     */
    public TestMetadataID(String name) {
        super(name);
    }

    private static String getTestVDBName() {
    	return UnitTestUtil.getTestDataPath() + "/ConnectorMetadata.vdb"; //$NON-NLS-1$
    }
    
    public static TranslationUtility createTranslationUtility(String vdbName) {
        return new TranslationUtility(vdbName);        
    }
    
    
    // ################ TEST GROUP METADATAID ######################
    
    public MetadataID getGroupID(String groupName, TranslationUtility transUtil) {
        IQuery query = (IQuery) transUtil.parseCommand("SELECT 1 FROM " + groupName); //$NON-NLS-1$
        IGroup group = (IGroup) query.getFrom().getItems().get(0);
        return group.getMetadataID();
    }

    public void helpTestGroupID(String fullGroupName, String shortGroupName, int elementCount, TranslationUtility transUtil) throws Exception {
        MetadataID groupID = getGroupID(fullGroupName, transUtil);     
        assertEquals(fullGroupName, groupID.getFullName());
        assertEquals(shortGroupName, groupID.getName());
        assertEquals(MetadataID.TYPE_GROUP, groupID.getType());
        assertNull(groupID.getParentID());
        
        // Check children
        List children = groupID.getChildIDs();
        assertEquals(elementCount, children.size());
        Iterator childIter = children.iterator();
        while(childIter.hasNext()) {
            MetadataID childID = (MetadataID) childIter.next();
            assertEquals(MetadataID.TYPE_ELEMENT, childID.getType());
            assertEquals(groupID, childID.getParentID());
            assertTrue(childID.getFullName().startsWith(groupID.getFullName()));            
        }
    }
    
    public void testGroupID() throws Exception {
        helpTestGroupID("ConnectorMetadata.TestTable", "TestTable", 7, CONNECTOR_METADATA_UTILITY);//$NON-NLS-1$ //$NON-NLS-2$ 
    }   

    public void testGroupID_longName() throws Exception {
        helpTestGroupID("ConnectorMetadata.TestCatalog.TestSchema.TestTable2", "TestTable2", 1, CONNECTOR_METADATA_UTILITY);//$NON-NLS-1$ //$NON-NLS-2$ 
    }   

    // ################ TEST ELEMENT METADATAID ######################
    
    public MetadataID getElementID(String groupName, String elementName, TranslationUtility transUtil) {
        IQuery query = (IQuery) transUtil.parseCommand("SELECT " + elementName + " FROM " + groupName); //$NON-NLS-1$ //$NON-NLS-2$
        ISelectSymbol symbol = (ISelectSymbol) query.getSelect().getSelectSymbols().get(0);
        IElement element = (IElement) symbol.getExpression();
        return element.getMetadataID();
    }
    
    public void helpTestElementID(String groupName, String elementName, TranslationUtility transUtil) throws Exception {
        MetadataID elementID = getElementID(groupName, elementName, transUtil);     
        assertEquals(MetadataID.TYPE_ELEMENT, elementID.getType());
        assertEquals(groupName + "." + elementName, elementID.getFullName()); //$NON-NLS-1$
        assertEquals(elementName, elementID.getName());
        assertEquals(Collections.EMPTY_LIST, elementID.getChildIDs());
        assertNotNull(elementID.getParentID());
        assertEquals(groupName, elementID.getParentID().getFullName());        
    }
    
    public void testElementID() throws Exception {
        helpTestElementID("ConnectorMetadata.TestTable", "TestNameInSource", CONNECTOR_METADATA_UTILITY);//$NON-NLS-1$ //$NON-NLS-2$ 
    }   

    public void testElementID_longName() throws Exception {
        helpTestElementID("ConnectorMetadata.TestCatalog.TestSchema.TestTable2", "TestCol", CONNECTOR_METADATA_UTILITY);//$NON-NLS-1$ //$NON-NLS-2$ 
    }   

    // ################ TEST PROCEDURE AND PARAMETER METADATAID ######################
    
    public MetadataID getProcedureID(String procName, int inputParamCount, TranslationUtility transUtil) {
        StringBuffer sql = new StringBuffer("EXEC "); //$NON-NLS-1$
        sql.append(procName);
        sql.append("("); //$NON-NLS-1$
        for(int i=0; i<inputParamCount; i++) {
            sql.append("null"); //$NON-NLS-1$
            if(i<(inputParamCount-1)) { 
                sql.append(", ");                 //$NON-NLS-1$
            }
        }
        sql.append(")"); //$NON-NLS-1$
        
        IProcedure proc = (IProcedure) transUtil.parseCommand(sql.toString()); 
        return proc.getMetadataID();
    }
    
    public void helpTestProcedureID(String procName, String shortName, int inputParamCount, String[] paramNames, TranslationUtility transUtil) throws Exception {
        MetadataID procID = getProcedureID(procName, inputParamCount, transUtil);     
        assertEquals(MetadataID.TYPE_PROCEDURE, procID.getType());
        assertEquals(procName, procID.getFullName()); //$NON-NLS-1$
        assertNull(procID.getParentID());
        assertEquals(shortName, procID.getName());
        
        // Check children
        List children = procID.getChildIDs();
        assertEquals(paramNames.length, children.size());
        Set actualParamNames = new HashSet();
        Iterator childIter = children.iterator();
        while(childIter.hasNext()) {
            MetadataID childID = (MetadataID) childIter.next();
            assertEquals(MetadataID.TYPE_PARAMETER, childID.getType());
            assertEquals(procID, childID.getParentID());
            assertTrue(childID.getFullName().startsWith(procID.getFullName()));
            
            actualParamNames.add(childID.getName());            
        }
        
        // Compare actual with expected param names
        Set expectedParamNames = new HashSet(Arrays.asList(paramNames));
        assertEquals(expectedParamNames, actualParamNames);
    }
    
    public void testProcedureID() throws Exception {
        String[] paramNames = new String[] { "InParam", "OutParam", "InOutParam", "ReturnParam" };          //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$//$NON-NLS-4$
        helpTestProcedureID("ConnectorMetadata.TestProc1", "TestProc1", 2, paramNames, CONNECTOR_METADATA_UTILITY); //$NON-NLS-1$ //$NON-NLS-2$               
    }

    public void testProcedureID_resultSet() throws Exception {
        String[] paramNames = new String[] { "Param1", "RSParam" };          //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$//$NON-NLS-4$
        helpTestProcedureID("ConnectorMetadata.TestProc2", "TestProc2", 1, paramNames, CONNECTOR_METADATA_UTILITY); //$NON-NLS-1$ //$NON-NLS-2$               
    }

    public void testProcedureID_longName() throws Exception {
        helpTestProcedureID("ConnectorMetadata.TestCatalog.TestSchema.TestProc", "TestProc", 0, new String[0], CONNECTOR_METADATA_UTILITY); //$NON-NLS-1$ //$NON-NLS-2$
    }


}
