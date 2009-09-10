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

package org.teiid.connector.metadata.runtime;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.Group;
import org.teiid.connector.metadata.runtime.Parameter;
import org.teiid.connector.metadata.runtime.Procedure;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.core.util.UnitTestUtil;

/**
 */
public class TestMetadataObject extends TestCase {

    private static TranslationUtility CONNECTOR_METADATA_UTILITY = createTranslationUtility(getTestVDBName());

    /**
     * Constructor for TestMetadataID.
     * @param name
     */
    public TestMetadataObject(String name) {
        super(name);
    }

    private static String getTestVDBName() {
    	return UnitTestUtil.getTestDataPath() + "/ConnectorMetadata.vdb"; //$NON-NLS-1$
    }
    
    public static TranslationUtility createTranslationUtility(String vdbName) {
        return new TranslationUtility(vdbName);        
    }
    
    
    // ################ TEST GROUP METADATAID ######################
    
    public Group getGroupID(String groupName, TranslationUtility transUtil) {
        IQuery query = (IQuery) transUtil.parseCommand("SELECT 1 FROM " + groupName); //$NON-NLS-1$
        IGroup group = (IGroup) query.getFrom().getItems().get(0);
        return group.getMetadataObject();
    }

    public void helpTestGroupID(String fullGroupName, String shortGroupName, int elementCount, TranslationUtility transUtil) throws Exception {
        Group groupID = getGroupID(fullGroupName, transUtil);     
        assertEquals(fullGroupName, groupID.getFullName());
        assertEquals(shortGroupName, groupID.getName());
        
        // Check children
        List<Element> children = groupID.getChildren();
        assertEquals(elementCount, children.size());
        for (Element element : children) {
            assertEquals(groupID, element.getParent());
            assertTrue(element.getFullName().startsWith(groupID.getFullName()));            
        }
    }
    
    public void testGroupID() throws Exception {
        helpTestGroupID("ConnectorMetadata.TestTable", "TestTable", 7, CONNECTOR_METADATA_UTILITY);//$NON-NLS-1$ //$NON-NLS-2$ 
    }   

    public void testGroupID_longName() throws Exception {
        helpTestGroupID("ConnectorMetadata.TestCatalog.TestSchema.TestTable2", "TestTable2", 1, CONNECTOR_METADATA_UTILITY);//$NON-NLS-1$ //$NON-NLS-2$ 
    }   

    // ################ TEST ELEMENT METADATAID ######################
    
    public Element getElementID(String groupName, String elementName, TranslationUtility transUtil) {
        IQuery query = (IQuery) transUtil.parseCommand("SELECT " + elementName + " FROM " + groupName); //$NON-NLS-1$ //$NON-NLS-2$
        ISelectSymbol symbol = (ISelectSymbol) query.getSelect().getSelectSymbols().get(0);
        IElement element = (IElement) symbol.getExpression();
        return element.getMetadataObject();
    }
    
    public void helpTestElementID(String groupName, String elementName, TranslationUtility transUtil) throws Exception {
        Element elementID = getElementID(groupName, elementName, transUtil);     
        assertEquals(groupName + "." + elementName, elementID.getFullName()); //$NON-NLS-1$
        assertEquals(elementName, elementID.getName());
        assertNotNull(elementID.getParent());
        assertEquals(groupName, elementID.getParent().getFullName());        
    }
    
    public void testElementID() throws Exception {
        helpTestElementID("ConnectorMetadata.TestTable", "TestNameInSource", CONNECTOR_METADATA_UTILITY);//$NON-NLS-1$ //$NON-NLS-2$ 
    }   

    public void testElementID_longName() throws Exception {
        helpTestElementID("ConnectorMetadata.TestCatalog.TestSchema.TestTable2", "TestCol", CONNECTOR_METADATA_UTILITY);//$NON-NLS-1$ //$NON-NLS-2$ 
    }   

    // ################ TEST PROCEDURE AND PARAMETER METADATAID ######################
    
    public Procedure getProcedureID(String procName, int inputParamCount, TranslationUtility transUtil) {
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
        return proc.getMetadataObject();
    }
    
    public void helpTestProcedureID(String procName, String shortName, int inputParamCount, String[] paramNames, TranslationUtility transUtil) throws Exception {
        Procedure procID = getProcedureID(procName, inputParamCount, transUtil);     
        assertEquals(procName, procID.getFullName()); //$NON-NLS-1$
        assertEquals(shortName, procID.getName());
        
        // Check children
        List<Parameter> children = procID.getChildren();
        assertEquals(paramNames.length, children.size());
        Set actualParamNames = new HashSet();
        for (Parameter childID : children) {
            assertEquals(procID, childID.getParent());
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
