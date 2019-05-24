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

package org.teiid.connector.metadata.runtime;

import java.util.List;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Call;
import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.metadata.Column;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Table;


/**
 */
public class TestMetadataObject {

    private static TranslationUtility CONNECTOR_METADATA_UTILITY = createTranslationUtility(getTestVDBName());

    private static String getTestVDBName() {
        return UnitTestUtil.getTestDataPath() + "/ConnectorMetadata.vdb"; //$NON-NLS-1$
    }

    public static TranslationUtility createTranslationUtility(String vdbName) {
        return new TranslationUtility(vdbName);
    }


    // ################ TEST GROUP METADATAID ######################

    public Table getGroupID(String groupName, TranslationUtility transUtil) {
        Select query = (Select) transUtil.parseCommand("SELECT 1 FROM " + groupName); //$NON-NLS-1$
        NamedTable group = (NamedTable) query.getFrom().get(0);
        return group.getMetadataObject();
    }

    public void helpTestGroupID(String fullGroupName, String shortGroupName, int elementCount, TranslationUtility transUtil) throws Exception {
        Table groupID = getGroupID(fullGroupName, transUtil);
        assertEquals(fullGroupName, groupID.getFullName());
        assertEquals(shortGroupName, groupID.getName());
        // Check children
        List<Column> children = groupID.getColumns();
        assertEquals(elementCount, children.size());
        for (Column element : children) {
            assertEquals(groupID, element.getParent());
            assertTrue(element.getFullName().startsWith(groupID.getFullName()));
        }
    }

    @Test public void testGroupID() throws Exception {
        helpTestGroupID("ConnectorMetadata.TestTable", "TestTable", 7, CONNECTOR_METADATA_UTILITY);//$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testGroupID_longName() throws Exception {
        helpTestGroupID("ConnectorMetadata.TestCatalog.TestSchema.TestTable2", "TestCatalog.TestSchema.TestTable2", 1, CONNECTOR_METADATA_UTILITY);//$NON-NLS-1$ //$NON-NLS-2$
    }

    // ################ TEST ELEMENT METADATAID ######################

    public Column getElementID(String groupName, String elementName, TranslationUtility transUtil) {
        Select query = (Select) transUtil.parseCommand("SELECT " + elementName + " FROM " + groupName); //$NON-NLS-1$ //$NON-NLS-2$
        DerivedColumn symbol = query.getDerivedColumns().get(0);
        ColumnReference element = (ColumnReference) symbol.getExpression();
        return element.getMetadataObject();
    }

    public void helpTestElementID(String groupName, String elementName, TranslationUtility transUtil) throws Exception {
        Column elementID = getElementID(groupName, elementName, transUtil);
        assertEquals(groupName + "." + elementName, elementID.getFullName()); //$NON-NLS-1$
        assertEquals(elementName, elementID.getName());
        assertNotNull(elementID.getParent());
        assertEquals(groupName, elementID.getParent().getFullName());
    }

    @Test public void testElementID() throws Exception {
        helpTestElementID("ConnectorMetadata.TestTable", "TestNameInSource", CONNECTOR_METADATA_UTILITY);//$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testElementID_longName() throws Exception {
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

        Call proc = (Call) transUtil.parseCommand(sql.toString());
        return proc.getMetadataObject();
    }

    public void helpTestProcedureID(String procName, String shortName, int inputParamCount, String[] paramNames, String rsParamName, TranslationUtility transUtil) throws Exception {
        Procedure procID = getProcedureID(procName, inputParamCount, transUtil);
        assertEquals(procName, procID.getFullName());
        assertEquals(shortName, procID.getName());

        // Check children
        List<ProcedureParameter> children = procID.getParameters();
        int i = 0;
        for (ProcedureParameter childID : children) {
            assertEquals(procID, childID.getParent());
            assertTrue(childID.getFullName() + " " + procID.getFullName(), childID.getFullName().startsWith(procID.getFullName())); //$NON-NLS-1$
            assertEquals(paramNames[i++], childID.getName());
        }

        if (rsParamName != null) {
            assertEquals(rsParamName, procID.getResultSet().getName());
        } else {
            assertNull(procID.getResultSet());
        }
    }

    @Test public void testProcedureID() throws Exception {
        String[] paramNames = new String[] { "InParam", "OutParam", "InOutParam", "ReturnParam" };          //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$//$NON-NLS-4$
        helpTestProcedureID("ConnectorMetadata.TestProc1", "TestProc1", 2, paramNames, null, CONNECTOR_METADATA_UTILITY); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testProcedureID_resultSet() throws Exception {
        String[] paramNames = new String[] { "Param1"};          //$NON-NLS-1$
        helpTestProcedureID("ConnectorMetadata.TestProc2", "TestProc2", 1, paramNames, "RSParam", CONNECTOR_METADATA_UTILITY); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testProcedureID_longName() throws Exception {
        helpTestProcedureID("ConnectorMetadata.TestCatalog.TestSchema.TestProc", "TestCatalog.TestSchema.TestProc", 0, new String[0], null, CONNECTOR_METADATA_UTILITY); //$NON-NLS-1$ //$NON-NLS-2$
    }


}
