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

package com.metamatrix.vdb.edit.materialization;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import junit.framework.TestCase;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.StringUtilities;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.vdb.materialization.DatabaseDialect;
import com.metamatrix.vdb.materialization.template.ExpandedTemplate;
import com.metamatrix.vdb.materialization.template.MaterializedViewData;
import com.metamatrix.vdb.materialization.template.Template;
import com.metamatrix.vdb.materialization.template.TemplateExpander;

public class TestGenerateScripts extends TestCase {

    private void helpTestTemplate(String command, DatabaseDialect database, TemplateExpander expander) {
        ExpandedTemplate template = expander.expand("aVDBName", getTemplateReaders(command, database), command); //$NON-NLS-1$
        String actual = template.contents.trim();
        checkScript(actual, database, command);
    }

    public void test() {
        assertEquals("viewName=view1", new TemplateExpander(getData(), DatabaseDialect.SQL_SERVER).expandText("viewName=$viewName$")); //$NON-NLS-1$ //$NON-NLS-2$
    }

//    public void testExpandFromReader() {
//        Reader[] readers = new Reader[] {new StringReader("group simple;\ntest(viewName, columnNames, virtualGroupName, materializationTableNameInSrc, materializationTableName, materializationStageTableNameInSrc, databaseDialect) ::= <<viewName=$viewName$>>")}; //$NON-NLS-1$
//        assertStringsMatch("viewName=view1", new TemplateExpander(getData(), DatabaseDialect.ORACLE).expandFromReaders(readers, "test")); //$NON-NLS-1$ //$NON-NLS-2$
//    }

    // ==================================================================================
    // Test generation of truncate script for all platforms
    // ==================================================================================
    public void testTruncateDB2Template() {
        helpTestTemplate(Template.TRUNCATE, DatabaseDialect.DB2, new TemplateExpander(getData(), DatabaseDialect.DB2));
    }

    public void testTruncateOracleTemplate() {
        helpTestTemplate(Template.TRUNCATE, DatabaseDialect.ORACLE, new TemplateExpander(getData(), DatabaseDialect.ORACLE));
    }

    public void testTruncateSQLServerTemplate() {
        helpTestTemplate(Template.TRUNCATE, DatabaseDialect.SQL_SERVER, new TemplateExpander(getData(), DatabaseDialect.SQL_SERVER));
    }

    // ==================================================================================
    // Test generation of load script for MetaMatrix platform
    // ==================================================================================
    public void testLoadMetaMatrixTemplate() {
        helpTestTemplate(Template.LOAD, DatabaseDialect.METAMATRIX, new TemplateExpander(getData(), DatabaseDialect.METAMATRIX));
    }

    // ==================================================================================
    // Test generation of swap script for all platforms
    // ==================================================================================
    public void testSwapDB2Template() {
        helpTestTemplate(Template.SWAP, DatabaseDialect.DB2, new TemplateExpander(getData(), DatabaseDialect.DB2));
    }

    public void testSwapOracleTemplate() {
        helpTestTemplate(Template.SWAP, DatabaseDialect.ORACLE, new TemplateExpander(getData(), DatabaseDialect.ORACLE));
    }

    public void testSwapSQLServerTemplate() {
        helpTestTemplate(Template.SWAP, DatabaseDialect.SQL_SERVER, new TemplateExpander(getData(), DatabaseDialect.SQL_SERVER));
    }

    // ==================================================================================
    // Test utilities
    // ==================================================================================
    private Reader[] getTemplateReaders(String command, DatabaseDialect database) {
        return new Reader[] {getReader("scriptMaterializedView"), getReader("loadMaterializedView_" + database)}; //$NON-NLS-1$ //$NON-NLS-2$
    }

    private static InputStreamReader getReader(String fileName) {
        String templateName = "com/metamatrix/vdb/materialization/template/" + fileName + ".stg"; //$NON-NLS-1$ //$NON-NLS-2$
        InputStream inputStream = TestGenerateScripts.class.getClassLoader().getResourceAsStream(templateName);
        if ( inputStream == null ) {
            throw new MetaMatrixRuntimeException("Unable to find resource: " + templateName); //$NON-NLS-1$
        }
        return new InputStreamReader(inputStream);
    }

    private MaterializedViewData getData() {
        MaterializedViewData data = new MaterializedViewData("view1", new String[] {"column1", //$NON-NLS-1$ //$NON-NLS-2$
                                                                                     "column2", //$NON-NLS-1$
                                                                                     "column3"}, //$NON-NLS-1$
                                                             "virtualGroupName", "materializationTableNameInSrc", //$NON-NLS-1$ //$NON-NLS-2$
                                                             "materializationTable",  //$NON-NLS-1$
                                                             "materializationStageTableNameInSrc", //$NON-NLS-1$
                                                             "materializationStageTableName"); //$NON-NLS-1$
        return data;
    }

    private void checkScript(String actual, DatabaseDialect database, String command) {
        String expected = new FileUtil(UnitTestUtil.getTestDataPath() + "/materializedView/expected/" + database + "_" + command + ".txt").read().trim(); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals(StringUtilities.removeChars(expected, new char[] {'\r'}), actual.trim());
    }
 }
