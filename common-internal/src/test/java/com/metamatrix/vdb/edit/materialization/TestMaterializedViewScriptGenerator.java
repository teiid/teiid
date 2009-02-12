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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.StringUtilities;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.vdb.materialization.DatabaseDialect;
import com.metamatrix.vdb.materialization.MaterializedViewScriptGenerator;
import com.metamatrix.vdb.materialization.MaterializedViewScriptGeneratorImpl;
import com.metamatrix.vdb.materialization.ScriptType;
import com.metamatrix.vdb.materialization.template.MaterializedViewConnectionData;
import com.metamatrix.vdb.materialization.template.MaterializedViewData;
import com.metamatrix.vdb.materialization.template.TemplateData;

public class TestMaterializedViewScriptGenerator extends TestCase {
    private static final String VDB_NAME = "MatviewTheFirst"; //$NON-NLS-1$
    private static final String VDB_SSL_NAME = "MatviewSSLTheFirst"; //$NON-NLS-1$
    private static final String VDB_VERSION_1 = "1"; //$NON-NLS-1$
    private static final String VDB_VERSION_2 = "2"; //$NON-NLS-1$
    
    // ==================================================================================
    // Test generation of load script
    // ==================================================================================

    public void testGenerateLoadScript() throws Exception {
        TemplateData loadTemplateData = helpCreateMatViewTemplateData(VDB_NAME);
        MaterializedViewScriptGenerator loadScriptGen = new MaterializedViewScriptGeneratorImpl(loadTemplateData);
        String scriptFileName = ScriptType.loadScriptFileName(VDB_NAME, VDB_VERSION_1);
        try {
            OutputStream oStream = new FileOutputStream(UnitTestUtil.getTestScratchFile(scriptFileName));
            loadScriptGen.generateMaterializationLoadScript(oStream);
        } catch (IOException err) {
            fail("Error generating the load script: " + err.getMessage()); //$NON-NLS-1$
        }
        checkScript(scriptFileName, scriptFileName);
    }

    // ==================================================================================
    // Test generation of swap script for all platforms (in one file)
    // ==================================================================================

    public void testGenerateConnOraPropFile() throws Exception {
        TemplateData connectionTemplateData = helpCreateOraConnPropsTemplateData(VDB_NAME);
        MaterializedViewScriptGenerator swapScriptGen = new MaterializedViewScriptGeneratorImpl(connectionTemplateData);
        String propertyFileName = ScriptType.connectionPropertyFileName(VDB_NAME, VDB_VERSION_2);
        OutputStream oStream = new FileOutputStream(UnitTestUtil.getTestScratchFile(propertyFileName));
        swapScriptGen.generateMaterializationConnectionPropFile(oStream);
        // load as Properties and check
        checkProperties(propertyFileName, propertyFileName);
    }
    
    
    public void testGenerateConnSQLServerPropFile() throws Exception {
        TemplateData connectionTemplateData = helpCreateSQLServerConnPropsTemplateData(VDB_NAME);
        MaterializedViewScriptGenerator swapScriptGen = new MaterializedViewScriptGeneratorImpl(connectionTemplateData);
        String propertyFileName = ScriptType.connectionPropertyFileName(VDB_NAME, VDB_VERSION_1);
        OutputStream oStream = new FileOutputStream(UnitTestUtil.getTestScratchFile(propertyFileName));
        swapScriptGen.generateMaterializationConnectionPropFile(oStream);
        // load as Properties and check
        checkProperties(propertyFileName, propertyFileName);
    }
    
    public void testGenerateConnSQLServerPropFileUsingSSL() throws Exception {
        TemplateData connectionTemplateData = helpCreateSQLServerConnPropsTemplateDataUsingSSL(VDB_SSL_NAME);
        MaterializedViewScriptGenerator swapScriptGen = new MaterializedViewScriptGeneratorImpl(connectionTemplateData);
        String propertyFileName = ScriptType.connectionPropertyFileName(VDB_SSL_NAME, VDB_VERSION_1);
        OutputStream oStream = new FileOutputStream(UnitTestUtil.getTestScratchFile(propertyFileName));
        swapScriptGen.generateMaterializationConnectionPropFile(oStream);
        // load as Properties and check
        checkProperties(propertyFileName, propertyFileName);
    }    
    
    // ==================================================================================
    // Test generation of scripts for all materialization models - for all db platforms
    // ==================================================================================

    /**
     *  
     * @throws Exception
     * @since 4.2
     */
    public void testGenerateScriptsByDBMSType() throws Exception {
        
        // Collection of TempateData - one for each materialization in VDB
        Collection templateData = helpCreateMatViewTemplateDataCollection();

        // ================================
        // Foreach supported database type
        // ================================
        Iterator databaseItr = DatabaseDialect.getAllDialects().iterator();
        while ( databaseItr.hasNext() ) {
            DatabaseDialect aDialect = (DatabaseDialect)databaseItr.next();
            helpTestScriptForMaterializedTable(templateData, aDialect);
        }
    }

    // ==================================================================================
    // Helpers
    // ==================================================================================

    /** 
     * @param templateData
     * @param aDialect
     * @throws Exception
     * @since 4.2
     */
    private void helpTestScriptForMaterializedTable(Collection templateData,
                                           DatabaseDialect aDialect) throws Exception {
        // File name and stream for Truncate file
        String truncFileName = ScriptType.truncateScriptFileName(aDialect, VDB_NAME);
        OutputStream truncStream = new FileOutputStream(UnitTestUtil.getTestScratchFile(truncFileName));

        // File name and stream for Swap file
        String swapFileName = ScriptType.swapScriptFileName(aDialect, VDB_NAME);
        OutputStream swapStream = new FileOutputStream(UnitTestUtil.getTestScratchFile(swapFileName));

        // ================================
        // Foreach materialization model
        // ================================
        Iterator materializationDataItr = templateData.iterator();
        while ( materializationDataItr.hasNext() ) {
        
            MaterializedViewScriptGenerator scriptGen = new MaterializedViewScriptGeneratorImpl((TemplateData)materializationDataItr.next());

            // Gen and add a trunc script to truncStream for this materialization
            scriptGen.generateMaterializationTruncateScript(truncStream, aDialect);

            // Gen and add a swap script to truncStream for this materialization
            scriptGen.generateMaterializationSwapScript(swapStream, aDialect);
        }
        truncStream.close();
        swapStream.close();
        
        // Check results
        checkScript(truncFileName, truncFileName);
        checkScript(swapFileName, swapFileName);
    }

    /** 
     * @return
     * @since 4.2
     */
    private Collection helpCreateMatViewTemplateDataCollection() {
        Collection data = new ArrayList();
        data.add(helpCreateMatViewTemplateData("View_1")); //$NON-NLS-1$
        data.add(helpCreateMatViewTemplateData("View_2")); //$NON-NLS-1$
        data.add(helpCreateMatViewTemplateData("View_3")); //$NON-NLS-1$
        return data;
    }

    private TemplateData helpCreateMatViewTemplateData(String vdbName) {
        return new MaterializedViewData(vdbName, 
                                        new String[] {"col_1", "col_2", "col_3", "col_4", "col_5"},  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
                                        "A_virtual_table",  //$NON-NLS-1$
                                        "A_physical_table_in_src",  //$NON-NLS-1$
                                        "A_physical_table",  //$NON-NLS-1$
                                        "A_physical_staging_table_in_src",  //$NON-NLS-1$
                                        "A_physical_staging_table"); //$NON-NLS-1$
    }
    
    private TemplateData helpCreateOraConnPropsTemplateData(String vdbName) {
        return new MaterializedViewConnectionData(vdbName, "vers_1", "host1", "123432",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                                  "com.metamatrix.jdbc.MMDriver", "aMMPwd", "aMMUser", "mm",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                                                  "jdbc:mmx:oracle://host:1521;SID=sid",  //$NON-NLS-1$
                                                  "com.metamatrix.jdbc.oracle.OracleDriver", "matPwd",  //$NON-NLS-1$ //$NON-NLS-2$
                                                  "matUser", "truncScript.DDL", "loadScript.DDL", "swapScript.DDL", "scrips.log"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
     
    
    private TemplateData helpCreateSQLServerConnPropsTemplateData(String vdbName) {
        return new MaterializedViewConnectionData(vdbName, "vers_1", "host1", "123432",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                                  "com.metamatrix.jdbc.MMDriver", "aMMPwd", "aMMUser", "mm", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                                                  "jdbc:mmx:sqlserver://host:1521;Database=somebogus\\database",  //$NON-NLS-1$
                                                  "com.metamatrix.jdbc.sqlserver.SQLServerDriver", "matPwd",  //$NON-NLS-1$ //$NON-NLS-2$
                                                  "matUser", "truncScript.DDL", "loadScript.DDL", "swapScript.DDL", "scrips.log"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
    
    private TemplateData helpCreateSQLServerConnPropsTemplateDataUsingSSL(String vdbName) {
        return new MaterializedViewConnectionData(vdbName, "vers_1", "host1", "123432",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                                  "com.metamatrix.jdbc.MMDriver", "aMMPwd", "aMMUser", "mms", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                                                  "jdbc:mmx:sqlserver://host:1521;Database=somebogus\\database",  //$NON-NLS-1$
                                                  "com.metamatrix.jdbc.sqlserver.SQLServerDriver", "matPwd",  //$NON-NLS-1$ //$NON-NLS-2$
                                                  "matUser", "truncScript.DDL", "loadScript.DDL", "swapScript.DDL", "scrips.log"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }    
    
    private void checkScript(String actualFileName, 
                             String expectedFileName) throws FileNotFoundException {
    	File actualFile = UnitTestUtil.getTestScratchFile(actualFileName);
    	
        String actual = FileUtil.read(new FileReader(actualFile)).trim();
        String expected = FileUtil.read(new FileReader(UnitTestUtil.getTestDataFile("/materializedView/expected/" + expectedFileName))).trim(); //$NON-NLS-1$ 
        assertEquals(StringUtilities.removeChars(expected, new char[] {'\r'}), actual);
        actualFile.delete();
    }
    
    private void checkProperties(String actualPropertyFileName, String expectedPropertyFileName) throws IOException {
    	
        Properties actualProps = new Properties();
        File actualFile = UnitTestUtil.getTestScratchFile(actualPropertyFileName);
        actualProps.load(new FileInputStream(actualFile));

        Properties expectedProps = new Properties();
        expectedProps.load(new FileInputStream(UnitTestUtil.getTestDataPath() + "/materializedView/expected/" + expectedPropertyFileName)); //$NON-NLS-1$

        Collection actualReadPropKeys = actualProps.keySet();
        Collection expectedReadPropKeys = expectedProps.keySet();
        
        assertEquals("Number of properties differ in prop files: ", expectedReadPropKeys.size(), actualReadPropKeys.size()); //$NON-NLS-1$
        
        assertTrue("Expected props is missing some actual prop keys: ", expectedReadPropKeys.containsAll(actualReadPropKeys)); //$NON-NLS-1$
        
        assertTrue("Actual props is missing some expecte prop keys: ", actualReadPropKeys.containsAll(expectedReadPropKeys)); //$NON-NLS-1$
        
        for ( Iterator expectedPropItr = expectedReadPropKeys.iterator(); expectedPropItr.hasNext(); ) {
            String aPropKey = (String)expectedPropItr.next();
            String expectedPropValue = expectedProps.getProperty(aPropKey);
            String actualPropValue = actualProps.getProperty(aPropKey);
            assertEquals("Wrong value for key " + aPropKey, expectedPropValue, actualPropValue); //$NON-NLS-1$
        }
        
        actualFile.delete();
    }

    /**
     * Utility to take a file name string (without any extension) and return a new name
     * with all non letter or digit characters replaced by an underscore.
     * @param fileName
     * @return
     * @since 4.2
     */
    protected String createValidFileName(final String fileName) {
        // Go through the string and ensure that each character is valid ...
        StringBuffer sb = new StringBuffer(100);
        CharacterIterator charIter = new StringCharacterIterator(fileName);
        char c = charIter.first();

        // The remaining characters must be either alphabetic or digit character ...
        while ( c != CharacterIterator.DONE ) {
            if ( Character.isLetterOrDigit(c) ) {
                sb.append(c);
            } else {
                sb.append('_');
            }
            c = charIter.next();    
        }
        
        return sb.toString();
    }
    
}
