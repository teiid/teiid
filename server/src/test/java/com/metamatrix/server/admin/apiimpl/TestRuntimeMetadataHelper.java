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

package com.metamatrix.server.admin.apiimpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.cache.FakeCache;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.core.vdb.ModelType;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.server.admin.api.MaterializationLoadScripts;
import com.metamatrix.vdb.materialization.DatabaseDialect;
import com.metamatrix.vdb.materialization.ScriptType;


/**
 * @since 4.2
 */
public class TestRuntimeMetadataHelper extends TestCase {

    private ModelInfo fakeMaterializationModel;

    protected void setUp() throws Exception {
        super.setUp();
        fakeMaterializationModel = new FakeModelInfo(ModelType.MATERIALIZATION);

        try {
        	RuntimeMetadataCatalog.getInstance().init(new Properties(), Mockito.mock(MessageBus.class), new FakeCache.FakeCacheFactory());
        } catch (VirtualDatabaseException e) {
        	//ignore, this should be related to the extension model manager
        }
    }

    //=================================================================================
    // Tests
    //=================================================================================

    public void testParseDatabaseType_Ora() {
        String oraURL = "jdbc:mmx:oracle://slntds04:1521;SID=ds04;DatabaseName=jcunningham_txn_test"; //$NON-NLS-1$
        String expected = "oracle"; //$NON-NLS-1$
        String actual = RuntimeMetadataHelper.parseDatabaseType(oraURL, DatabaseDialect.ORACLE.getDriverClassname());
        assertEquals(expected, actual);
        assertCannonicalStringsMatch(expected, actual);
    }

    public void testParseDatabaseType_SQLServer() {
        String sqlURL = "jdbc:mmx:sqlserver://slntds05:1433;DatabaseName=jcunningham_txn_test"; //$NON-NLS-1$
        String expected = "sqlserver"; //$NON-NLS-1$
        String actual = RuntimeMetadataHelper.parseDatabaseType(sqlURL, DatabaseDialect.SQL_SERVER.getDriverClassname());
        assertEquals(expected, actual);
        assertCannonicalStringsMatch(expected, actual);
    }

    public void testParseDatabaseType_DB2() {
        String db2URL = "jdbc:mmx:db2://slntds05:50000;DatabaseName=ds05;"; //$NON-NLS-1$
        String expected = "db2"; //$NON-NLS-1$
        String actual = RuntimeMetadataHelper.parseDatabaseType(db2URL, DatabaseDialect.DB2.getDriverClassname());
        assertEquals(expected, actual);
        assertCannonicalStringsMatch(expected, actual);
    }

    public void testParseDatabaseType_SYBASE() {
        String db2URL = "jdbc:mmx:sybase://slntds17:5000;DatabaseName=dv_vhalbert2"; //$NON-NLS-1$
        String expected = "sybase"; //$NON-NLS-1$
        String actual = RuntimeMetadataHelper.parseDatabaseType(db2URL, DatabaseDialect.SYBASE.getDriverClassname());
        assertEquals(expected, actual);
        assertCannonicalStringsMatch(expected, actual);
    }

    public void testParseDatabaseType_MySQL() {
        String mysqlURL = "jdbc:mysql://slntds03:3306/rep_unit_test"; //$NON-NLS-1$
        String expected = "mysql"; //$NON-NLS-1$
        String actual = RuntimeMetadataHelper.parseDatabaseType(mysqlURL, DatabaseDialect.MYSQL.getDriverClassname());
        assertEquals(expected, actual);
        assertCannonicalStringsMatch(expected, actual);
    }

    public void testCreateMaterializedViewLoadPropertiesOracle() throws Exception {
        Map ddlFiles = setupMaterializationModelDDLFiles();
        fakeMaterializationModel.setDDLFiles(ddlFiles);

        String vdbName = "TestConnPropsVDB"; //$NON-NLS-1$
        String vdbVersion = null;

        String expectedLoadScriptFileName = ScriptType.connectionPropertyFileName(vdbName, "1"); //$NON-NLS-1$

        MaterializationLoadScripts loadScript = RuntimeMetadataHelper.createMaterializedViewLoadProperties(fakeMaterializationModel,
                                                                   "jdbc:mmx:oracle://slntds04:1521;SID=ds04", //$NON-NLS-1$
                                                                   "com.metamatrix.jdbc.oracle.OracleDriver",  //$NON-NLS-1$
                                                                   "matUser","matPwd",   //$NON-NLS-1$ //$NON-NLS-2$
                                                                   "mmHost1", "12345",  //$NON-NLS-1$ //$NON-NLS-2$
                                                                   "com.metamatrix.jdbc.MMDriver",  //$NON-NLS-1$
                                                                   false,
                                                                   "aMMUser", "aMMPwd",  //$NON-NLS-1$ //$NON-NLS-2$
                                                                   vdbName, vdbVersion);

        String actualFileName = loadScript.getConnectionPropsFileName();
        InputStream connPropFileStream = loadScript.getConnectionPropsFileContents();
        helpSaveProps(actualFileName, connPropFileStream);

        // load as Properties and verify properties are equal
        checkProperties(actualFileName, expectedLoadScriptFileName);

        String expectedFileName = ScriptType.createScriptFileName(vdbName, "1"); //$NON-NLS-1$

        // check that create script file names are equal
        assertEquals("Expected create script file names to be equal.", expectedFileName, loadScript.getCreateScriptFileName()); //$NON-NLS-1$


        // check that create scripts are equal
        checkFileContents("Expected create scripts to be equal.",  //$NON-NLS-1$
                          (byte[])ddlFiles.get("MaterializationModel_Oracle_8i_9i_DDL.ddl"),  //$NON-NLS-1$
                          ByteArrayHelper.toByteArray(loadScript.getCreateScriptFile()));
    }

    public void testCreateMaterializedViewLoadPropertiesOracleUsingSSL() throws Exception {
        CurrentConfiguration.getInstance().getConfiguration();
        
        Map ddlFiles = setupMaterializationModelDDLFiles();
        fakeMaterializationModel.setDDLFiles(ddlFiles);

        String vdbName = "TestSSLConnPropsVDB"; //$NON-NLS-1$
        String vdbVersion = null;

        String expectedLoadScriptFileName = ScriptType.connectionPropertyFileName(vdbName, "1"); //$NON-NLS-1$

        MaterializationLoadScripts loadScript = RuntimeMetadataHelper.createMaterializedViewLoadProperties(fakeMaterializationModel,
                                                                   "jdbc:mmx:oracle://slntds04:1521;SID=ds04", //$NON-NLS-1$
                                                                   "com.metamatrix.jdbc.oracle.OracleDriver",  //$NON-NLS-1$
                                                                   "matUser","matPwd",   //$NON-NLS-1$ //$NON-NLS-2$
                                                                   "mmHost1", "12345",  //$NON-NLS-1$ //$NON-NLS-2$
                                                                   "com.metamatrix.jdbc.MMDriver",  //$NON-NLS-1$
                                                                   true,
                                                                   "aMMUser", "aMMPwd",  //$NON-NLS-1$ //$NON-NLS-2$
                                                                   vdbName, vdbVersion);

        String actualFileName = loadScript.getConnectionPropsFileName();
        InputStream connPropFileStream = loadScript.getConnectionPropsFileContents();
        helpSaveProps(actualFileName, connPropFileStream);

        // load as Properties and verify properties are equal
        checkProperties(actualFileName, expectedLoadScriptFileName);

        String expectedFileName = ScriptType.createScriptFileName(vdbName, "1"); //$NON-NLS-1$

        // check that create script file names are equal
        assertEquals("Expected create script file names to be equal.", expectedFileName, loadScript.getCreateScriptFileName()); //$NON-NLS-1$


        // check that create scripts are equal
        checkFileContents("Expected create scripts to be equal.",  //$NON-NLS-1$
                          (byte[])ddlFiles.get("MaterializationModel_Oracle_8i_9i_DDL.ddl"),  //$NON-NLS-1$
                          ByteArrayHelper.toByteArray(loadScript.getCreateScriptFile()));
    }

    public void testCreateMaterializedViewLoadPropertiesSQLServer() throws Exception {
        Map ddlFiles = setupMaterializationModelDDLFilesReal();
        fakeMaterializationModel.setDDLFiles(ddlFiles);

        String vdbName = "PartsSupplier"; //$NON-NLS-1$
        String vdbVersion = "3"; //$NON-NLS-1$

        String expectedLoadScriptFileName = ScriptType.connectionPropertyFileName(vdbName, vdbVersion);

        MaterializationLoadScripts loadScript = RuntimeMetadataHelper.createMaterializedViewLoadProperties(fakeMaterializationModel,
                                                                   "jdbc:mmx:sqlserver://slntds05:1433;DatabaseName=jcunningham_txn_test\\cha", //$NON-NLS-1$
                                                                   "com.metamatrix.jdbc.sqlserver.SQLServerDriver",  //$NON-NLS-1$
                                                                   "matUser","matPwd",   //$NON-NLS-1$ //$NON-NLS-2$
                                                                   "mmHost1", "12345",  //$NON-NLS-1$ //$NON-NLS-2$
                                                                   "com.metamatrix.jdbc.MMDriver",  //$NON-NLS-1$
                                                                   false,
                                                                   "aMMUser", "aMMPwd",  //$NON-NLS-1$ //$NON-NLS-2$
                                                                   vdbName, vdbVersion);

        String actualFileName = loadScript.getConnectionPropsFileName();
        InputStream connPropFileStream = loadScript.getConnectionPropsFileContents();
        helpSaveProps(actualFileName, connPropFileStream);

        // load as Properties and verify properties are equal
        checkProperties(actualFileName, expectedLoadScriptFileName);

        String expectedFileName = ScriptType.createScriptFileName(vdbName, vdbVersion);

        // check that create script file names are equal
        assertEquals("Expected create script file names to be equal.",  //$NON-NLS-1$
                     expectedFileName,
                     loadScript.getCreateScriptFileName());


        // check that create scripts are equal
        checkFileContents("Expected create scripts to be equal.",  //$NON-NLS-1$
                          (byte[])ddlFiles.get("MaterializationModel_Microsoft_SQL_Server_DDL.ddl"),  //$NON-NLS-1$
                          ByteArrayHelper.toByteArray(loadScript.getCreateScriptFile()));
    }

    public void testCreateMaterializedViewLoadPropertiesDB2() throws Exception {
        Map ddlFiles = setupMaterializationModelDDLFilesReal();
        fakeMaterializationModel.setDDLFiles(ddlFiles);

        String vdbName = "RiggaMaRoll"; //$NON-NLS-1$
        String vdbVersion = "33"; //$NON-NLS-1$

        String expectedLoadScriptFileName = ScriptType.connectionPropertyFileName(vdbName, vdbVersion);

        MaterializationLoadScripts loadScript = RuntimeMetadataHelper.createMaterializedViewLoadProperties(fakeMaterializationModel,
                                                                   "jdbc:mmx:db2://slntds05:50000;DatabaseName=ds05;", //$NON-NLS-1$
                                                                   "com.metamatrix.jdbc.db2.DB2Driver",  //$NON-NLS-1$
                                                                   "matUser","matPwd",   //$NON-NLS-1$ //$NON-NLS-2$
                                                                   "mmHost1", "12345",  //$NON-NLS-1$ //$NON-NLS-2$
                                                                   "com.metamatrix.jdbc.MMDriver",  //$NON-NLS-1$
                                                                   false,
                                                                   "aMMUser", "aMMPwd",  //$NON-NLS-1$ //$NON-NLS-2$
                                                                   vdbName, vdbVersion);

        String actualFileName = loadScript.getConnectionPropsFileName();
        InputStream connPropFileStream = loadScript.getConnectionPropsFileContents();
        helpSaveProps(actualFileName, connPropFileStream);
        // load as Properties and check
        checkProperties(actualFileName, expectedLoadScriptFileName);

        String expectedFileName = ScriptType.createScriptFileName(vdbName, vdbVersion);

        // load as Properties and verify properties are equal
        assertEquals("Expected create script file names to be equal.",  //$NON-NLS-1$
                     expectedFileName,
                     loadScript.getCreateScriptFileName());


        // check that create scripts are equal
        checkFileContents("Expected create scripts to be equal.",  //$NON-NLS-1$
                          (byte[])ddlFiles.get("MaterializationModel_IBM_DB2_7_x_DDL.ddl"),  //$NON-NLS-1$
                          ByteArrayHelper.toByteArray(loadScript.getCreateScriptFile()));
    }

    public void testCreateMaterializedViewLoadPropertiesMySQL() throws Exception {
        Map ddlFiles = setupMaterializationModelDDLFilesReal();
        fakeMaterializationModel.setDDLFiles(ddlFiles);

        String vdbName = "MySQL"; //$NON-NLS-1$
        String vdbVersion = "1"; //$NON-NLS-1$

        String expectedLoadScriptFileName = ScriptType.connectionPropertyFileName(vdbName, vdbVersion);

        MaterializationLoadScripts loadScript = RuntimeMetadataHelper.createMaterializedViewLoadProperties(fakeMaterializationModel,
                                                                   "jdbc:mysql://slntds03:3306/rep_unit_test", //$NON-NLS-1$
                                                                   "com.mysql.jdbc.Driver",  //$NON-NLS-1$
                                                                   "rep_unit_test","mm",   //$NON-NLS-1$ //$NON-NLS-2$
                                                                   "mmHost1", "12345",  //$NON-NLS-1$ //$NON-NLS-2$
                                                                   "com.metamatrix.jdbc.MMDriver",  //$NON-NLS-1$
                                                                   false,
                                                                   "aMMUser", "aMMPwd",  //$NON-NLS-1$ //$NON-NLS-2$
                                                                   vdbName, vdbVersion);

        String actualFileName = loadScript.getConnectionPropsFileName();
        InputStream connPropFileStream = loadScript.getConnectionPropsFileContents();
        helpSaveProps(actualFileName, connPropFileStream);
        // load as Properties and check
        checkProperties(actualFileName, expectedLoadScriptFileName);

        String expectedFileName = ScriptType.createScriptFileName(vdbName, vdbVersion);

        // load as Properties and verify properties are equal
        assertEquals("Expected create script file names to be equal.",  //$NON-NLS-1$
                     expectedFileName,
                     loadScript.getCreateScriptFileName());


        // check that create scripts are equal
        checkFileContents("Expected create scripts to be equal.",  //$NON-NLS-1$
                          (byte[])ddlFiles.get("MaterializationModel_MySQL_x_DDL.ddl"),  //$NON-NLS-1$
                          ByteArrayHelper.toByteArray(loadScript.getCreateScriptFile()));
    }

    //=================================================================================
    // Helpers
    //=================================================================================

    private void assertCannonicalStringsMatch(String expected, String actual) {
        assertEquals(expected.toUpperCase(), actual.toUpperCase());
    }

    /**
     * @param actualFileName
     * @param connPropFileStream
     * @throws IOException
     * @since 4.2
     */
    private void helpSaveProps(String actualFileName,
                               InputStream connPropFileStream) throws IOException {
        File scratchFile = UnitTestUtil.getTestScratchFile(actualFileName);
        FileUtils.write(connPropFileStream, scratchFile);
    }

    private void checkProperties(String actualPropertyFileName,
                                 String expectedPropertyFileName) throws IOException {

        Properties actualProps = new Properties();
        actualProps.load(new FileInputStream(UnitTestUtil.getTestScratchPath() + UnitTestUtil.PATH_SEPARATOR + actualPropertyFileName));

        Properties expectedProps = new Properties();
        expectedProps.load(new FileInputStream(UnitTestUtil.getTestDataPath() + "/materializedView/expected/" + expectedPropertyFileName)); //$NON-NLS-1$

        Collection actualReadPropKeys = actualProps.keySet();
        Collection expectedReadPropKeys = expectedProps.keySet();

        assertEquals("Number of properties differ in prop files: ", expectedReadPropKeys.size(), actualReadPropKeys.size()); //$NON-NLS-1$

        assertTrue("Expected props is missing some actual prop keys: ", expectedReadPropKeys.containsAll(actualReadPropKeys)); //$NON-NLS-1$

        assertTrue("Actual props is missing some expecte prop keys: ", actualReadPropKeys.containsAll(expectedReadPropKeys)); //$NON-NLS-1$

        Map non_matchingProps = new HashMap();
        for ( Iterator expectedPropItr = expectedReadPropKeys.iterator(); expectedPropItr.hasNext(); ) {
            String aPropKey = (String)expectedPropItr.next();
            String expectedPropValue = expectedProps.getProperty(aPropKey);
            String actualPropValue = actualProps.getProperty(aPropKey);
            if ( ! expectedPropValue.equals(actualPropValue) ) {
                non_matchingProps.put(aPropKey, "<" + expectedPropValue + ">\n<" +actualPropValue +">");  //$NON-NLS-1$ //$NON-NLS-2$//$NON-NLS-3$
            }
        }

        if ( ! non_matchingProps.isEmpty() ) {
            StringBuffer buf = new StringBuffer();
            for ( Iterator itr = non_matchingProps.keySet().iterator(); itr.hasNext(); ) {
                String name = (String)itr.next();
                buf.append('[');
                buf.append(name);
                buf.append(":\n"); //$NON-NLS-1$
                buf.append(non_matchingProps.get(name));
                buf.append("]\n\n"); //$NON-NLS-1$
            }
            fail("These prop values didn't match:\n[propName:\n<expected>\n<actual>]\n" + buf.toString()); //$NON-NLS-1$
        }
    }

    /**
     * @param string
     * @param string2
     * @param createScriptFile
     * @since 4.2
     */
    private void checkFileContents(String string,
                                   byte[] expectedFileConts,
                                   byte[] actualFileContents) {
        String theExpected = new String(expectedFileConts);
        String theActual = new String(actualFileContents);
       //System.out.println("*** Expected [" + theExpected + "] Actual [" + theActual + "]");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$

        assertEquals("Expected create scripts to be equal.", theExpected, theActual); //$NON-NLS-1$
    }

    protected Map setupMaterializationModelDDLFilesReal() {
        Map ddlFileNamesToFiles = new HashMap();

        String scriptContents = "// " + DatabaseDialect.ORACLE + " truncate script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put("Oracle_Truncate_materialized_PS.DDL", scriptContents.getBytes()); //$NON-NLS-1$
        scriptContents = "// " + DatabaseDialect.ORACLE + " swap script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put("Oracle_Swap_materialized_PS.DDL", scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// " + DatabaseDialect.DB2 + " truncate script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put("DB2_Truncate_materialized_PS.DDL", scriptContents.getBytes()); //$NON-NLS-1$
        scriptContents = "// " + DatabaseDialect.DB2 + " swap script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put("DB2_Swap_materialized_PS.DDL", scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// " + DatabaseDialect.SQL_SERVER + " truncate script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put("SqlServer_Truncate_materialized_PS.DDL", scriptContents.getBytes()); //$NON-NLS-1$
        scriptContents = "// " + DatabaseDialect.SQL_SERVER + " swap script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put("SqlServer_Swap_materialized_PS.DDL", scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// " + DatabaseDialect.MYSQL + " truncate script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put("MySQL_Truncate_materialized_PS.DDL", scriptContents.getBytes()); //$NON-NLS-1$
        scriptContents = "// " + DatabaseDialect.MYSQL + " swap script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put("MySQL_Swap_materialized_PS.DDL", scriptContents.getBytes()); //$NON-NLS-1$


        scriptContents = "// MetaMatrix load script"; //$NON-NLS-1$
        ddlFileNamesToFiles.put("MetaMatrix_Load_materialized_PS.DDL", scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// Oracle create DDL script"; //$NON-NLS-1$
        ddlFileNamesToFiles.put("MaterializationModel_Oracle_8i_9i_DDL.ddl", scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// DB2 create DDL script"; //$NON-NLS-1$
        ddlFileNamesToFiles.put("MaterializationModel_IBM_DB2_7_x_DDL.ddl", scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// Generic create DDL script"; //$NON-NLS-1$
        ddlFileNamesToFiles.put("MaterializationModel_Passthrough__intermediate_XML_form_.ddl", scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// SQL Server create DDL script"; //$NON-NLS-1$
        ddlFileNamesToFiles.put("MaterializationModel_Microsoft_SQL_Server_DDL.ddl", scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// MySQL create DDL script"; //$NON-NLS-1$
        ddlFileNamesToFiles.put("MaterializationModel_MySQL_x_DDL.ddl", scriptContents.getBytes()); //$NON-NLS-1$

        return ddlFileNamesToFiles;
    }

    protected Map setupMaterializationModelDDLFiles() {
        Map ddlFileNamesToFiles = new HashMap();

        String scriptContents = "// " + DatabaseDialect.ORACLE + " truncate script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put(DatabaseDialect.ORACLE + "_" + ScriptType.MATERIALIZATION_TRUNCATE_SCRIPT_FILE_PREFIX, scriptContents.getBytes()); //$NON-NLS-1$
        scriptContents = "// " + DatabaseDialect.ORACLE + " swap script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put(DatabaseDialect.ORACLE + "_" + ScriptType.MATERIALIZATION_SWAP_SCRIPT_FILE_PREFIX, scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// " + DatabaseDialect.DB2 + " truncate script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put(DatabaseDialect.DB2 + "_" + ScriptType.MATERIALIZATION_TRUNCATE_SCRIPT_FILE_PREFIX, scriptContents.getBytes()); //$NON-NLS-1$
        scriptContents = "// " + DatabaseDialect.DB2 + " swap script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put(DatabaseDialect.DB2 + "_" + ScriptType.MATERIALIZATION_SWAP_SCRIPT_FILE_PREFIX, scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// " + DatabaseDialect.SQL_SERVER + " truncate script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put(DatabaseDialect.SQL_SERVER + "_" + ScriptType.MATERIALIZATION_TRUNCATE_SCRIPT_FILE_PREFIX, scriptContents.getBytes()); //$NON-NLS-1$
        scriptContents = "// " + DatabaseDialect.SQL_SERVER + " swap script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put(DatabaseDialect.SQL_SERVER + "_" + ScriptType.MATERIALIZATION_SWAP_SCRIPT_FILE_PREFIX, scriptContents.getBytes()); //$NON-NLS-1$


        scriptContents = "// " + DatabaseDialect.MYSQL + " truncate script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put(DatabaseDialect.MYSQL + "_" + ScriptType.MATERIALIZATION_TRUNCATE_SCRIPT_FILE_PREFIX, scriptContents.getBytes()); //$NON-NLS-1$
        scriptContents = "// " + DatabaseDialect.MYSQL + " swap script"; //$NON-NLS-1$ //$NON-NLS-2$
        ddlFileNamesToFiles.put(DatabaseDialect.MYSQL + "_" + ScriptType.MATERIALIZATION_SWAP_SCRIPT_FILE_PREFIX, scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// MetaMatrix load script"; //$NON-NLS-1$
        ddlFileNamesToFiles.put("MetaMatrix_" + ScriptType.MATERIALIZATION_LOAD_SCRIPT_FILE_PREFIX, scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// Oracle create DDL script"; //$NON-NLS-1$
        ddlFileNamesToFiles.put("MaterializationModel_Oracle_8i_9i_DDL.ddl", scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// DB2 create DDL script"; //$NON-NLS-1$
        ddlFileNamesToFiles.put("MaterializationModel_IBM_DB2_7_x_DDL.ddl", scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// Generic create DDL script"; //$NON-NLS-1$
        ddlFileNamesToFiles.put("MaterializationModel_Passthrough__intermediate_XML_form_.ddl", scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// SQL Serve create DDL script"; //$NON-NLS-1$
        ddlFileNamesToFiles.put("MaterializationModel_Microsoft_SQL_Server_DDL.ddl", scriptContents.getBytes()); //$NON-NLS-1$

        scriptContents = "// MySQL create DDL script"; //$NON-NLS-1$
        ddlFileNamesToFiles.put("MaterializationModel_MySQL_x_DDL.ddl", scriptContents.getBytes()); //$NON-NLS-1$

        return ddlFileNamesToFiles;
    }

//    protected void printMap(Map map) {
//        StringBuffer buf = new StringBuffer();
//        List keys = new ArrayList(map.keySet());
//        Collections.sort(keys);
//        for (final Iterator iter = keys.iterator(); iter.hasNext();) {
//            final String key = (String)iter.next();
//            buf.append(key);
//            buf.append(" <---> "); //$NON-NLS-1$
//            buf.append(map.get(key));
//            buf.append('\n');
//        }
//        System.out.println("\nDDL Files:\n"); //$NON-NLS-1$
//        System.out.println(buf.toString());
//    }
}
